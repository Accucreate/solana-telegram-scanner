import os
import time
import json
import threading
import queue
import requests
import websocket

from datetime import datetime, timezone


# ============================================================
# SETTINGS
# ============================================================

MIN_MC = 5_000
MAX_MC = 100_000

MIN_LIQUIDITY = 5_000
MIN_HOLDERS = 100

# Maximum time we monitor a new Pump.fun token
MONITOR_MAX_MINUTES = 30

# How often the pending monitor runs
MONITOR_INTERVAL = 5

# Maximum tokens waiting in memory
MAX_PENDING_TOKENS = 300

# Number of workers
WORKER_COUNT = 2

# Minimum time between DexScreener requests
# This is deliberately conservative to avoid 429 errors.
DEX_REQUEST_GAP = 1.5

# Normal request timeout
REQUEST_TIMEOUT = 20

# Retry settings
MAX_RETRIES = 4

# PumpPortal
PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"


# ============================================================
# API URLS
# ============================================================

SOLANA_RPC = "https://api.mainnet-beta.solana.com"

DEXSCREENER_TOKEN = (
    "https://api.dexscreener.com/latest/dex/tokens/"
)

RUGCHECK_REPORT = (
    "https://api.rugcheck.xyz/v1/tokens/{}/report"
)

TELEGRAM_API = (
    "https://api.telegram.org/bot{}/sendMessage"
)


# ============================================================
# FILES
# ============================================================

SEEN_FILE = "seen_tokens.json"


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN"
)

TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID"
)


# ============================================================
# RISK SETTINGS
# ============================================================

TOP10_GREEN = 30
TOP10_YELLOW = 45

LIQ_MC_GREEN = 30
LIQ_MC_YELLOW = 15

VOL_MC_GREEN_MAX = 5
VOL_MC_YELLOW_MAX = 10

HOLDERS_GREEN = 500
HOLDERS_YELLOW = 100

AGE_GREEN = 60
AGE_YELLOW = 15


# ============================================================
# HTTP SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "SolanaPumpScanner/7.0",
    "Accept": "application/json"
})


# ============================================================
# GLOBAL STATE
# ============================================================

seen_lock = threading.Lock()
pending_lock = threading.Lock()

seen = set()

pending = {}

token_queue = queue.Queue(
    maxsize=MAX_PENDING_TOKENS
)

# Prevents duplicate queue entries
queued_tokens = set()

queued_lock = threading.Lock()

# Last time we queried DexScreener
dex_lock = threading.Lock()
last_dex_request = 0.0


# ============================================================
# LOGGING
# ============================================================

def log(message):

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"[{now}] {message}",
        flush=True
    )


# ============================================================
# HELPERS
# ============================================================

def safe_float(value, default=0):

    try:

        if value is None:
            return default

        if isinstance(value, str):

            value = value.replace(",", "")
            value = value.replace("$", "")

        return float(value)

    except Exception:

        return default


def format_money(value):

    value = safe_float(value)

    if value >= 1_000_000:

        return f"${value / 1_000_000:.2f}M"

    if value >= 1_000:

        return f"${value:,.0f}"

    return f"${value:.2f}"


def format_number(value):

    try:

        return f"{int(float(value)):,}"

    except Exception:

        return "Unknown"


# ============================================================
# RATE LIMITER
# ============================================================

def wait_for_dex_slot():

    global last_dex_request

    with dex_lock:

        now = time.time()

        wait_time = (
            DEX_REQUEST_GAP
            - (now - last_dex_request)
        )

        if wait_time > 0:

            time.sleep(wait_time)

        last_dex_request = time.time()


# ============================================================
# HTTP GET
# ============================================================

def request_get(
    url,
    service="generic",
    **kwargs
):

    timeout = kwargs.pop(
        "timeout",
        REQUEST_TIMEOUT
    )

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            # DexScreener gets a global
            # request limiter.
            if service == "dex":

                wait_for_dex_slot()

            response = session.get(
                url,
                timeout=timeout,
                **kwargs
            )

            # ------------------------------------------------
            # RATE LIMITED
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                if retry_after:

                    try:
                        wait = float(
                            retry_after
                        )
                    except Exception:
                        wait = 10
                else:

                    # Exponential backoff
                    wait = min(
                        10 * (2 ** (attempt - 1)),
                        60
                    )

                log(
                    f"⚠️ {service} rate limited "
                    f"(429). Waiting {wait:.1f}s..."
                )

                time.sleep(wait)

                continue

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            log(
                f"GET {service} attempt "
                f"{attempt}/{MAX_RETRIES} failed: "
                f"{str(e)[:180]}"
            )

            if attempt < MAX_RETRIES:

                wait = min(
                    3 * attempt,
                    15
                )

                time.sleep(wait)

    return None


# ============================================================
# HTTP POST
# ============================================================

def request_post(
    url,
    **kwargs
):

    timeout = kwargs.pop(
        "timeout",
        REQUEST_TIMEOUT
    )

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = session.post(
                url,
                timeout=timeout,
                **kwargs
            )

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:

                    wait = (
                        float(retry_after)
                        if retry_after
                        else 5
                    )

                except Exception:

                    wait = 5

                log(
                    f"⚠️ POST rate limited. "
                    f"Waiting {wait:.1f}s..."
                )

                time.sleep(wait)

                continue

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            log(
                f"POST attempt "
                f"{attempt}/{MAX_RETRIES} failed: "
                f"{str(e)[:180]}"
            )

            if attempt < MAX_RETRIES:

                time.sleep(
                    min(3 * attempt, 15)
                )

    return None


# ============================================================
# SEEN TOKENS
# ============================================================

def load_seen():

    try:

        if not os.path.exists(
            SEEN_FILE
        ):

            return set()

        with open(
            SEEN_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        if isinstance(data, list):

            return set(data)

    except Exception as e:

        log(
            f"Could not load seen tokens: {e}"
        )

    return set()


def save_seen():

    try:

        with seen_lock:

            data = sorted(
                list(seen)
            )

        with open(
            SEEN_FILE,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                data,
                file,
                indent=2
            )

    except Exception as e:

        log(
            f"Could not save seen tokens: {e}"
        )


# ============================================================
# SOLANA RPC
# ============================================================

def rpc_call(
    method,
    params
):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    response = request_post(
        SOLANA_RPC,
        json=payload
    )

    if not response:

        return None

    try:

        data = response.json()

        if data.get("error"):

            log(
                f"RPC error: "
                f"{data['error']}"
            )

            return None

        return data.get("result")

    except Exception as e:

        log(
            f"RPC JSON error: {e}"
        )

        return None


# ============================================================
# TOKEN AUTHORITIES
# ============================================================

def get_token_authorities(mint):

    result = {
        "mint_authority": None,
        "freeze_authority": None,
        "success": False
    }

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            mint,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed"
            }
        ]
    }

    response = request_post(
        SOLANA_RPC,
        json=payload
    )

    if not response:

        return result

    try:

        data = response.json()

        value = (
            data
            .get("result", {})
            .get("value")
        )

        if not value:

            return result

        account_data = (
            value.get("data")
            or {}
        )

        parsed = (
            account_data.get("parsed")
            or {}
        )

        info = (
            parsed.get("info")
            or {}
        )

        if parsed.get("type") != "mint":

            return result

        result["mint_authority"] = (
            info.get("mintAuthority")
        )

        result["freeze_authority"] = (
            info.get("freezeAuthority")
        )

        result["success"] = True

        return result

    except Exception as e:

        log(
            f"Authority error: {e}"
        )

        return result


def authority_status(authority):

    if authority is None:
        return "Revoked"

    if authority == "":
        return "Revoked"

    return "Active"


# ============================================================
# DEXSCREENER
# ============================================================

def get_token_pair(mint):

    response = request_get(
        DEXSCREENER_TOKEN + mint,
        service="dex"
    )

    if not response:

        return None

    try:

        data = response.json()

        pairs = (
            data.get("pairs")
            or []
        )

        solana_pairs = [
            pair
            for pair in pairs
            if pair.get("chainId") == "solana"
        ]

        if not solana_pairs:

            return None

        solana_pairs.sort(
            key=lambda p: safe_float(
                (
                    p.get("liquidity")
                    or {}
                ).get("usd")
            ),
            reverse=True
        )

        return solana_pairs[0]

    except Exception as e:

        log(
            f"DexScreener JSON error: {e}"
        )

        return None


# ============================================================
# RUGCHECK
# ============================================================

def get_rugcheck_data(mint):

    result = {
        "holders": None,
        "top10": None,
        "risk": None
    }

    response = request_get(
        RUGCHECK_REPORT.format(mint),
        service="rugcheck"
    )

    if not response:

        return result

    try:

        data = response.json()

        # ----------------------------------------------------
        # HOLDERS
        # ----------------------------------------------------

        holder_count = (
            data.get("totalHolders")
            or data.get("holderCount")
            or data.get("holdersCount")
        )

        if holder_count is not None:

            result["holders"] = holder_count

        # ----------------------------------------------------
        # TOP 10
        # ----------------------------------------------------

        top_holders = data.get(
            "topHolders"
        )

        if (
            isinstance(top_holders, list)
            and top_holders
        ):

            total_percentage = 0

            for holder in top_holders[:10]:

                if not isinstance(
                    holder,
                    dict
                ):

                    continue

                percentage = safe_float(
                    holder.get("pct")
                    or holder.get("percentage")
                    or holder.get(
                        "ownershipPercentage"
                    )
                )

                if 0 < percentage <= 1:

                    percentage *= 100

                total_percentage += percentage

            if total_percentage > 0:

                result["top10"] = (
                    total_percentage
                )

        # ----------------------------------------------------
        # RISKS
        # ----------------------------------------------------

        risks = data.get("risks")

        if isinstance(risks, list):

            names = []

            for risk in risks:

                if not isinstance(
                    risk,
                    dict
                ):

                    continue

                name = (
                    risk.get("name")
                    or risk.get("description")
                    or risk.get("level")
                )

                if name:

                    names.append(
                        str(name)
                    )

            if names:

                result["risk"] = ", ".join(
                    names[:5]
                )

        return result

    except Exception as e:

        log(
            f"RugCheck JSON error: {e}"
        )

        return result


# ============================================================
# AGE
# ============================================================

def get_age_minutes(pair):

    try:

        created = pair.get(
            "pairCreatedAt"
        )

        if not created:

            return None

        created_seconds = (
            float(created) / 1000
        )

        now = datetime.now(
            timezone.utc
        ).timestamp()

        age_seconds = max(
            0,
            now - created_seconds
        )

        return int(
            age_seconds / 60
        )

    except Exception:

        return None


def format_age(age_minutes):

    if age_minutes is None:

        return "Unknown"

    if age_minutes < 60:

        return f"{age_minutes} min"

    hours = age_minutes // 60

    if hours < 24:

        return f"{hours} hr"

    days = hours // 24

    return f"{days} day"


# ============================================================
# RISK FLAGS
# ============================================================

def evaluate_top10(top10):

    if top10 is None:

        return "⚪ Top 10: Unknown"

    if top10 <= TOP10_GREEN:

        return f"🟢 Top 10: {top10:.1f}%"

    if top10 <= TOP10_YELLOW:

        return f"🟡 Top 10: {top10:.1f}%"

    return f"🔴 Top 10: {top10:.1f}%"


def evaluate_liquidity_mc(
    liquidity,
    mc
):

    if mc <= 0:

        return "⚪ Liquidity/MC: Unknown"

    ratio = (
        liquidity / mc
    ) * 100

    if ratio >= LIQ_MC_GREEN:

        emoji = "🟢"

    elif ratio >= LIQ_MC_YELLOW:

        emoji = "🟡"

    else:

        emoji = "🔴"

    return (
        f"{emoji} Liquidity/MC: "
        f"{ratio:.1f}%"
    )


def evaluate_volume_mc(
    volume,
    mc
):

    if mc <= 0:

        return "⚪ Volume/MC: Unknown"

    ratio = volume / mc

    if ratio <= VOL_MC_GREEN_MAX:

        emoji = "🟢"

    elif ratio <= VOL_MC_YELLOW_MAX:

        emoji = "🟡"

    else:

        emoji = "🔴"

    return (
        f"{emoji} Volume/MC: "
        f"{ratio:.1f}x"
    )


def evaluate_holders(holders):

    if holders is None:

        return "⚪ Holders: Unknown"

    holders = int(
        safe_float(holders)
    )

    if holders >= HOLDERS_GREEN:

        emoji = "🟢"

    elif holders >= HOLDERS_YELLOW:

        emoji = "🟡"

    else:

        emoji = "🔴"

    return (
        f"{emoji} Holders: "
        f"{holders:,}"
    )


def evaluate_age(age_minutes):

    if age_minutes is None:

        return "⚪ Age: Unknown"

    if age_minutes >= AGE_GREEN:

        emoji = "🟢"

    elif age_minutes >= AGE_YELLOW:

        emoji = "🟡"

    else:

        emoji = "🔴"

    return (
        f"{emoji} Age: "
        f"{format_age(age_minutes)}"
    )


def calculate_overall_risk(levels):

    high = levels.count("HIGH")
    medium = levels.count("MEDIUM")

    if high >= 2:

        return "HIGH"

    if high >= 1 and medium >= 1:

        return "HIGH"

    if high == 1:

        return "MEDIUM"

    if medium >= 1:

        return "MEDIUM"

    return "LOW"


def build_risk_flags(
    top10,
    liquidity,
    mc,
    volume,
    holders,
    age_minutes
):

    levels = []

    # TOP 10
    if top10 is None:

        top10_level = "UNKNOWN"

    elif top10 <= TOP10_GREEN:

        top10_level = "LOW"

    elif top10 <= TOP10_YELLOW:

        top10_level = "MEDIUM"

    else:

        top10_level = "HIGH"

    if top10_level != "UNKNOWN":

        levels.append(top10_level)

    # LIQUIDITY / MC
    if mc <= 0:

        liq_level = "UNKNOWN"

    else:

        ratio = (
            liquidity / mc
        ) * 100

        if ratio >= LIQ_MC_GREEN:

            liq_level = "LOW"

        elif ratio >= LIQ_MC_YELLOW:

            liq_level = "MEDIUM"

        else:

            liq_level = "HIGH"

    if liq_level != "UNKNOWN":

        levels.append(liq_level)

    # VOLUME / MC
    if mc <= 0:

        vol_level = "UNKNOWN"

    else:

        ratio = volume / mc

        if ratio <= VOL_MC_GREEN_MAX:

            vol_level = "LOW"

        elif ratio <= VOL_MC_YELLOW_MAX:

            vol_level = "MEDIUM"

        else:

            vol_level = "HIGH"

    if vol_level != "UNKNOWN":

        levels.append(vol_level)

    # HOLDERS
    if holders is None:

        holder_level = "UNKNOWN"

    elif holders >= HOLDERS_GREEN:

        holder_level = "LOW"

    elif holders >= HOLDERS_YELLOW:

        holder_level = "MEDIUM"

    else:

        holder_level = "HIGH"

    if holder_level != "UNKNOWN":

        levels.append(holder_level)

    # AGE
    if age_minutes is None:

        age_level = "UNKNOWN"

    elif age_minutes >= AGE_GREEN:

        age_level = "LOW"

    elif age_minutes >= AGE_YELLOW:

        age_level = "MEDIUM"

    else:

        age_level = "HIGH"

    if age_level != "UNKNOWN":

        levels.append(age_level)

    overall = calculate_overall_risk(
        levels
    )

    text = "\n".join([
        evaluate_top10(top10),
        evaluate_liquidity_mc(
            liquidity,
            mc
        ),
        evaluate_volume_mc(
            volume,
            mc
        ),
        evaluate_holders(
            holders
        ),
        evaluate_age(
            age_minutes
        )
    ])

    return text, overall


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:

        log(
            "❌ TELEGRAM_BOT_TOKEN missing"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        log(
            "❌ TELEGRAM_CHAT_ID missing"
        )

        return False

    url = TELEGRAM_API.format(
        TELEGRAM_BOT_TOKEN
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": False
    }

    response = request_post(
        url,
        json=payload
    )

    if not response:

        return False

    try:

        data = response.json()

        if data.get("ok"):

            return True

        log(
            f"Telegram API error: {data}"
        )

    except Exception:

        pass

    return False


# ============================================================
# BUILD ALERT
# ============================================================

def build_alert(
    pair,
    mint,
    security,
    authorities
):

    base_token = (
        pair.get("baseToken")
        or {}
    )

    name = (
        base_token.get("name")
        or "Unknown"
    )

    symbol = (
        base_token.get("symbol")
        or "UNKNOWN"
    )

    mc = safe_float(
        pair.get("marketCap")
        or pair.get("fdv")
    )

    liquidity = safe_float(
        (
            pair.get("liquidity")
            or {}
        ).get("usd")
    )

    volume = safe_float(
        (
            pair.get("volume")
            or {}
        ).get("h24")
    )

    holders = security.get(
        "holders"
    )

    holders_text = (
        format_number(holders)
        if holders is not None
        else "Unknown"
    )

    top10 = security.get(
        "top10"
    )

    top10_text = (
        f"{top10:.1f}%"
        if top10 is not None
        else "Unknown"
    )

    if authorities.get("success"):

        mint_status = authority_status(
            authorities.get(
                "mint_authority"
            )
        )

        freeze_status = authority_status(
            authorities.get(
                "freeze_authority"
            )
        )

    else:

        mint_status = "Unknown"
        freeze_status = "Unknown"

    age_minutes = get_age_minutes(
        pair
    )

    age = format_age(
        age_minutes
    )

    risk_text, overall = build_risk_flags(
        top10,
        liquidity,
        mc,
        volume,
        holders,
        age_minutes
    )

    if overall == "LOW":

        overall_emoji = "🟢"

    elif overall == "MEDIUM":

        overall_emoji = "🟡"

    else:

        overall_emoji = "🔴"

    dex_url = (
        pair.get("url")
        or
        f"https://dexscreener.com/solana/{mint}"
    )

    rug_url = (
        f"https://rugcheck.xyz/tokens/{mint}"
    )

    pump_url = (
        f"https://pump.fun/coin/{mint}"
    )

    message = f"""🚨 NEW PUMP.FUN TOKEN

Name: {name} ({symbol})

CA:
{mint}

💰 MC: {format_money(mc)}
💧 Liquidity: {format_money(liquidity)}
👥 Holders: {format_number(holders)}
⏱ Age: {age}
📊 Volume 24h: {format_money(volume)}

🔒 Mint: {mint_status}
❄️ Freeze: {freeze_status}
🐋 Top 10: {top10_text}

🛡️ RISK FLAGS

{risk_text}

{overall_emoji} OVERALL RISK: {overall}

🔗 Pump.fun:
{pump_url}

🔗 DexScreener:
{dex_url}

🔍 RugCheck:
{rug_url}

⚠️ DYOR — New/low-cap tokens are extremely risky.
"""

    return message


# ============================================================
# QUEUE MANAGEMENT
# ============================================================

def enqueue_token(
    mint,
    launch_data
):

    if not mint:

        return False

    with seen_lock:

        if mint in seen:

            return False

    with queued_lock:

        if mint in queued_tokens:

            return False

        queued_tokens.add(mint)

    try:

        token_queue.put_nowait({
            "mint": mint,
            "launch_data": launch_data
        })

        return True

    except queue.Full:

        with queued_lock:

            queued_tokens.discard(mint)

        return False


def remove_from_queue_state(mint):

    with queued_lock:

        queued_tokens.discard(mint)


# ============================================================
# CHECK TOKEN
# ============================================================

def check_token(
    mint,
    launch_data
):

    with seen_lock:

        if mint in seen:

            return

    pair = get_token_pair(
        mint
    )

    if not pair:

        log(
            f"⏳ No DexScreener pair yet: "
            f"{mint}"
        )

        return

    mc = safe_float(
        pair.get("marketCap")
        or pair.get("fdv")
    )

    liquidity = safe_float(
        (
            pair.get("liquidity")
            or {}
        ).get("usd")
    )

    age_minutes = get_age_minutes(
        pair
    )

    # --------------------------------------------------------
    # AGE
    # --------------------------------------------------------

    if age_minutes is not None:

        if age_minutes > MONITOR_MAX_MINUTES:

            log(
                f"⌛ Token expired: "
                f"{mint}"
            )

            with pending_lock:

                pending.pop(
                    mint,
                    None
                )

            return

    # --------------------------------------------------------
    # MC TOO HIGH
    # --------------------------------------------------------

    if mc > MAX_MC:

        log(
            f"⬆️ MC above "
            f"${MAX_MC:,}: "
            f"{mint} | "
            f"{format_money(mc)}"
        )

        with pending_lock:

            pending.pop(
                mint,
                None
            )

        return

    # --------------------------------------------------------
    # MC TOO LOW
    # --------------------------------------------------------

    if mc < MIN_MC:

        log(
            f"⏳ MC below "
            f"${MIN_MC:,}: "
            f"{mint} | "
            f"{format_money(mc)}"
        )

        return

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    if liquidity < MIN_LIQUIDITY:

        log(
            f"💧 Liquidity below "
            f"${MIN_LIQUIDITY:,}: "
            f"{mint} | "
            f"{format_money(liquidity)}"
        )

        return

    # --------------------------------------------------------
    # RUGCHECK
    # --------------------------------------------------------

    security = get_rugcheck_data(
        mint
    )

    holders = security.get(
        "holders"
    )

    if (
        holders is None
        or safe_float(holders) < MIN_HOLDERS
    ):

        log(
            f"👥 Holders below "
            f"{MIN_HOLDERS}: "
            f"{mint} | "
            f"{holders if holders is not None else 'Unknown'}"
        )

        return

    # --------------------------------------------------------
    # AUTHORITIES
    # --------------------------------------------------------

    authorities = get_token_authorities(
        mint
    )

    # --------------------------------------------------------
    # QUALIFIED
    # --------------------------------------------------------

    log(
        f"🔥 QUALIFIED TOKEN: "
        f"{mint} | "
        f"MC={format_money(mc)} | "
        f"Liquidity={format_money(liquidity)} | "
        f"Holders={format_number(holders)}"
    )

    message = build_alert(
        pair,
        mint,
        security,
        authorities
    )

    if send_telegram(message):

        log(
            f"✅ Telegram alert sent: "
            f"{mint}"
        )

        with seen_lock:

            seen.add(mint)

        save_seen()

        with pending_lock:

            pending.pop(
                mint,
                None
            )

    else:

        log(
            f"❌ Telegram failed: "
            f"{mint}"
        )


# ============================================================
# WORKER
# ============================================================

def worker():

    while True:

        try:

            item = token_queue.get()

            if item is None:

                token_queue.task_done()

                continue

            mint = item.get(
                "mint"
            )

            launch_data = item.get(
                "launch_data"
            )

            try:

                check_token(
                    mint,
                    launch_data
                )

            except Exception as e:

                log(
                    f"Worker error for "
                    f"{mint}: {e}"
                )

            finally:

                remove_from_queue_state(
                    mint
                )

                token_queue.task_done()

        except Exception as e:

            log(
                f"Worker loop error: {e}"
            )

            time.sleep(2)


# ============================================================
# PENDING MONITOR
# ============================================================

def pending_monitor():

    while True:

        try:

            time.sleep(
                MONITOR_INTERVAL
            )

            now = time.time()

            with pending_lock:

                items = list(
                    pending.items()
                )

            # ------------------------------------------------
            # Only enqueue a limited number
            # during each cycle.
            # ------------------------------------------------

            added = 0

            for mint, data in items:

                if added >= WORKER_COUNT:

                    break

                first_seen = data.get(
                    "first_seen",
                    now
                )

                age_seconds = (
                    now - first_seen
                )

                if age_seconds > (
                    MONITOR_MAX_MINUTES * 60
                ):

                    with pending_lock:

                        pending.pop(
                            mint,
                            None
                        )

                    log(
                        f"⌛ Monitoring expired: "
                        f"{mint}"
                    )

                    continue

                with queued_lock:

                    already_queued = (
                        mint in queued_tokens
                    )

                if already_queued:

                    continue

                if enqueue_token(
                    mint,
                    data.get(
                        "launch_data"
                    )
                ):

                    added += 1

        except Exception as e:

            log(
                f"Pending monitor error: {e}"
            )

            time.sleep(3)


# ============================================================
# ADD NEW TOKEN
# ============================================================

def add_new_token(event):

    mint = event.get(
        "mint"
    )

    if not mint:

        return

    with seen_lock:

        if mint in seen:

            return

    with pending_lock:

        if mint in pending:

            return

        # Keep pending list bounded.
        if len(pending) >= MAX_PENDING_TOKENS:

            log(
                "⚠️ Pending list full. "
                "Ignoring newest token temporarily."
            )

            return

        pending[mint] = {
            "first_seen": time.time(),
            "launch_data": event
        }

    name = (
        event.get("name")
        or "Unknown"
    )

    symbol = (
        event.get("symbol")
        or "UNKNOWN"
    )

    log(
        "🟢 NEW PUMP.FUN LAUNCH"
    )

    log(
        f"   Name: {name} ({symbol})"
    )

    log(
        f"   CA: {mint}"
    )

    log(
        f"   Dev: "
        f"{event.get('traderPublicKey', 'Unknown')}"
    )

    log(
        f"   Pump MC SOL: "
        f"{event.get('marketCapSol', 'Unknown')}"
    )

    # Try immediately once.
    if enqueue_token(
        mint,
        event
    ):

        return

    log(
        f"⏳ Added to pending monitor: "
        f"{mint}"
    )


# ============================================================
# WEBSOCKET STATE
# ============================================================

last_event_time = time.time()


# ============================================================
# WEBSOCKET MESSAGE
# ============================================================

def on_message(
    ws,
    message
):

    global last_event_time

    last_event_time = time.time()

    try:

        if not message:

            return

        data = json.loads(
            message
        )

        if not isinstance(
            data,
            dict
        ):

            return

        tx_type = data.get(
            "txType"
        )

        if tx_type != "create":

            return

        add_new_token(
            data
        )

    except json.JSONDecodeError:

        log(
            "⚠️ Invalid WebSocket JSON"
        )

    except Exception as e:

        log(
            f"WebSocket message error: {e}"
        )


# ============================================================
# WEBSOCKET ERROR
# ============================================================

def on_error(
    ws,
    error
):

    log(
        f"⚠️ PumpPortal WebSocket error: "
        f"{str(error)[:300]}"
    )


# ============================================================
# WEBSOCKET CLOSED
# ============================================================

def on_close(
    ws,
    close_status_code,
    close_msg
):

    log(
        f"🔌 PumpPortal connection closed: "
        f"{close_status_code} "
        f"{close_msg}"
    )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_open(ws):

    log(
        "🟢 Connected to PumpPortal."
    )

    subscribe_message = {
        "method": "subscribeNewToken"
    }

    ws.send(
        json.dumps(
            subscribe_message
        )
    )

    log(
        "📡 Live Pump.fun launch stream subscribed."
    )


# ============================================================
# WEBSOCKET LISTENER
# ============================================================

def websocket_listener():

    reconnect_delay = 3

    while True:

        try:

            log(
                "🔌 Connecting to PumpPortal..."
            )

            ws = websocket.WebSocketApp(
                PUMPPORTAL_WS,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
                ping_payload="pump-scanner"
            )

        except Exception as e:

            log(
                f"❌ WebSocket connection error: "
                f"{e}"
            )

        log(
            f"🔄 Reconnecting in "
            f"{reconnect_delay}s..."
        )

        time.sleep(
            reconnect_delay
        )

        reconnect_delay = min(
            reconnect_delay * 2,
            60
        )


# ============================================================
# WATCHDOG
# ============================================================

def websocket_watchdog():

    global last_event_time

    while True:

        try:

            time.sleep(60)

            silent_for = (
                time.time()
                - last_event_time
            )

            if silent_for > 600:

                log(
                    "⚠️ No Pump.fun launch event "
                    "for 10+ minutes."
                )

        except Exception as e:

            log(
                f"Watchdog error: {e}"
            )


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    for number in range(
        WORKER_COUNT
    ):

        thread = threading.Thread(
            target=worker,
            daemon=True
        )

        thread.start()

    log(
        f"👷 Started "
        f"{WORKER_COUNT} controlled workers."
    )


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            time.sleep(60)

            with pending_lock:

                pending_count = len(
                    pending
                )

            with seen_lock:

                seen_count = len(
                    seen
                )

            with queued_lock:

                queued_count = len(
                    queued_tokens
                )

            log(
                f"📊 STATUS | "
                f"Pending: {pending_count} | "
                f"Queued: {queued_count} | "
                f"Seen/Alerted: {seen_count} | "
                f"Queue: {token_queue.qsize()}"
            )

        except Exception as e:

            log(
                f"Status error: {e}"
            )


# ============================================================
# MAIN
# ============================================================

def main():

    global seen

    if not TELEGRAM_BOT_TOKEN:

        log(
            "❌ TELEGRAM_BOT_TOKEN is not set."
        )

        return

    if not TELEGRAM_CHAT_ID:

        log(
            "❌ TELEGRAM_CHAT_ID is not set."
        )

        return

    seen = load_seen()

    log(
        "=========================================================="
    )

    log(
        "🚀 REAL-TIME PUMP.FUN SCANNER v7"
    )

    log(
        "=========================================================="
    )

    log(
        "Detection: PumpPortal WebSocket"
    )

    log(
        "Mode: subscribeNewToken"
    )

    log(
        f"Market Cap: "
        f"${MIN_MC:,} - ${MAX_MC:,}"
    )

    log(
        f"Minimum Liquidity: "
        f"${MIN_LIQUIDITY:,}"
    )

    log(
        f"Minimum Holders: "
        f"{MIN_HOLDERS}"
    )

    log(
        f"Monitor window: "
        f"{MONITOR_MAX_MINUTES} minutes"
    )

    log(
        f"Dex request gap: "
        f"{DEX_REQUEST_GAP}s"
    )

    log(
        f"Workers: "
        f"{WORKER_COUNT}"
    )

    log(
        f"Previously alerted: "
        f"{len(seen)}"
    )

    log(
        "=========================================================="
    )

    start_workers()

    monitor_thread = threading.Thread(
        target=pending_monitor,
        daemon=True
    )

    monitor_thread.start()

    watchdog_thread = threading.Thread(
        target=websocket_watchdog,
        daemon=True
    )

    watchdog_thread.start()

    status_thread = threading.Thread(
        target=status_loop,
        daemon=True
    )

    status_thread.start()

    websocket_listener()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
