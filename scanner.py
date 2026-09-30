import os
import json
import time
import threading
import queue
from datetime import datetime

import requests
import websocket


# ============================================================
# CONFIG
# ============================================================

MIN_MC = 5_000
MAX_MC = 100_000
MIN_LIQUIDITY = 5_000
MIN_HOLDERS = 100

MONITOR_MAX_MINUTES = 30

MAX_PENDING_TOKENS = 500
MAX_QUEUE_SIZE = 30
WORKER_COUNT = 2

# Minimum delay between DexScreener requests
DEX_MIN_REQUEST_INTERVAL = 1.5

REQUEST_TIMEOUT = 20
MAX_RETRIES = 2
RETRY_DELAY = 2

PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

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

SEEN_FILE = "seen_tokens.json"


# ============================================================
# TELEGRAM
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ============================================================
# GLOBALS
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Mozilla/5.0 Solana-Pump-Scanner/3.0"
})

work_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)

pending_lock = threading.Lock()
seen_lock = threading.Lock()
dex_lock = threading.Lock()
stats_lock = threading.Lock()

pending_tokens = {}
seen_tokens = set()

last_dex_request = 0.0

stats = {
    "detected": 0,
    "checked": 0,
    "alerts": 0,
    "expired": 0,
    "dex_429": 0,
    "queue_full": 0,
}


# ============================================================
# LOG
# ============================================================

def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# SEEN TOKENS
# ============================================================

def load_seen():

    global seen_tokens

    try:

        if not os.path.exists(SEEN_FILE):
            seen_tokens = set()
            return

        with open(SEEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            seen_tokens = set(data)

        elif isinstance(data, dict):
            seen_tokens = set(data.keys())

        else:
            seen_tokens = set()

        log(
            f"📂 Loaded {len(seen_tokens)} seen tokens"
        )

    except Exception as e:

        log(f"⚠️ Could not load seen tokens: {e}")
        seen_tokens = set()


def save_seen():

    try:

        with seen_lock:
            data = list(seen_tokens)

        temp_file = SEEN_FILE + ".tmp"

        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f)

        os.replace(temp_file, SEEN_FILE)

    except Exception as e:

        log(f"⚠️ Could not save seen tokens: {e}")


def is_seen(address):

    with seen_lock:
        return address in seen_tokens


def mark_seen(address):

    with seen_lock:
        seen_tokens.add(address)

    save_seen()


# ============================================================
# HTTP GET
# ============================================================

def request_get(url, params=None):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_time = float(retry_after)
                except (TypeError, ValueError):
                    wait_time = 10

                with stats_lock:
                    stats["dex_429"] += 1

                log(
                    f"🛑 429 rate limit. "
                    f"Waiting {wait_time:.1f}s"
                )

                time.sleep(min(wait_time, 30))

                # IMPORTANT:
                # Don't immediately hammer the API again.
                return None

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            if attempt >= MAX_RETRIES:

                log(
                    f"⚠️ GET failed: {e}"
                )

                return None

            wait_time = RETRY_DELAY * attempt

            time.sleep(wait_time)

        except Exception as e:

            log(
                f"⚠️ Unexpected GET error: {e}"
            )

            return None

    return None


# ============================================================
# HTTP POST
# ============================================================

def request_post(url, json_data=None):

    for attempt in range(1, MAX_RETRIES + 1):

        try:

            response = session.post(
                url,
                json=json_data,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 429:

                retry_after = response.headers.get(
                    "Retry-After"
                )

                try:
                    wait_time = float(retry_after)
                except (TypeError, ValueError):
                    wait_time = 10

                log(
                    f"🛑 POST 429. "
                    f"Waiting {wait_time:.1f}s"
                )

                time.sleep(min(wait_time, 30))

                return None

            response.raise_for_status()

            return response

        except requests.RequestException:

            if attempt >= MAX_RETRIES:
                return None

            time.sleep(
                RETRY_DELAY * attempt
            )

        except Exception as e:

            log(
                f"⚠️ POST error: {e}"
            )

            return None

    return None


# ============================================================
# DEXSCREENER RATE LIMITER
# ============================================================

def dex_rate_limit():

    global last_dex_request

    with dex_lock:

        now = time.time()

        elapsed = now - last_dex_request

        if elapsed < DEX_MIN_REQUEST_INTERVAL:

            time.sleep(
                DEX_MIN_REQUEST_INTERVAL - elapsed
            )

        last_dex_request = time.time()


# ============================================================
# DEXSCREENER
# ============================================================

def get_token_pair(mint_address):

    dex_rate_limit()

    url = (
        DEXSCREENER_TOKEN
        + mint_address
    )

    response = request_get(url)

    if response is None:
        return None

    try:

        data = response.json()

        pairs = data.get("pairs") or []

        solana_pairs = [
            pair
            for pair in pairs
            if pair.get("chainId") == "solana"
        ]

        if not solana_pairs:
            return None

        # Select the pair with the highest liquidity
        best_pair = max(
            solana_pairs,
            key=lambda pair: float(
                (
                    pair.get("liquidity") or {}
                ).get("usd") or 0
            )
        )

        return best_pair

    except Exception as e:

        log(
            f"⚠️ DexScreener data error: {e}"
        )

        return None


# ============================================================
# SOLANA RPC
# ============================================================

def rpc_call(method, params):

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }

    response = request_post(
        SOLANA_RPC,
        payload
    )

    if response is None:
        return None

    try:
        return response.json()

    except Exception:
        return None


def get_token_authorities(mint_address):

    try:

        data = rpc_call(
            "getAccountInfo",
            [
                mint_address,
                {
                    "encoding": "jsonParsed"
                }
            ]
        )

        if not data:
            return "Unknown", "Unknown"

        result = (
            data
            .get("result", {})
            .get("value")
        )

        if not result:
            return "Unknown", "Unknown"

        parsed = (
            result
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )

        mint_authority = parsed.get(
            "mintAuthority"
        )

        freeze_authority = parsed.get(
            "freezeAuthority"
        )

        mint_status = (
            "Revoked"
            if mint_authority is None
            else "Active"
        )

        freeze_status = (
            "Revoked"
            if freeze_authority is None
            else "Active"
        )

        return (
            mint_status,
            freeze_status
        )

    except Exception as e:

        log(
            f"⚠️ Authority check failed: {e}"
        )

        return "Unknown", "Unknown"


# ============================================================
# RUGCHECK
# ============================================================

def get_rugcheck_data(mint_address):

    url = RUGCHECK_REPORT.format(
        mint_address
    )

    response = request_get(url)

    if response is None:

        return {
            "holders": None,
            "top10": None,
            "risks": []
        }

    try:

        data = response.json()

        # ----------------------------------------------------
        # HOLDERS
        # ----------------------------------------------------

        holders = data.get(
            "totalHolders"
        )

        if holders is None:
            holders = data.get(
                "holdersCount"
            )

        if holders is None:
            holders = data.get(
                "holderCount"
            )

        try:

            holders = (
                int(holders)
                if holders is not None
                else None
            )

        except Exception:

            holders = None

        # ----------------------------------------------------
        # TOP 10
        # ----------------------------------------------------

        top10 = None

        possible_top10 = [
            data.get("top10"),
            data.get("top10Holders"),
            data.get("topHoldersPercentage")
        ]

        for value in possible_top10:

            if value is None:
                continue

            try:

                top10 = float(value)

                if top10 <= 1:
                    top10 *= 100

                break

            except Exception:
                pass

        # ----------------------------------------------------
        # TOP HOLDER ARRAY
        # ----------------------------------------------------

        if top10 is None:

            top_holders = data.get(
                "topHolders"
            )

            if isinstance(
                top_holders,
                list
            ):

                total = 0.0

                for holder in top_holders[:10]:

                    if not isinstance(
                        holder,
                        dict
                    ):
                        continue

                    value = holder.get(
                        "pct"
                    )

                    if value is None:
                        value = holder.get(
                            "percentage"
                        )

                    try:

                        value = float(value)

                        if value <= 1:
                            value *= 100

                        total += value

                    except Exception:
                        continue

                if total > 0:
                    top10 = total

        # ----------------------------------------------------
        # RUGCHECK RISKS
        # ----------------------------------------------------

        risks = []

        raw_risks = data.get(
            "risks"
        )

        if isinstance(
            raw_risks,
            list
        ):

            for risk in raw_risks[:10]:

                if isinstance(
                    risk,
                    dict
                ):

                    text_value = (
                        risk.get("name")
                        or risk.get("description")
                    )

                    if text_value:
                        risks.append(
                            str(text_value)
                        )

                else:

                    risks.append(
                        str(risk)
                    )

        return {
            "holders": holders,
            "top10": top10,
            "risks": risks
        }

    except Exception as e:

        log(
            f"⚠️ RugCheck error: {e}"
        )

        return {
            "holders": None,
            "top10": None,
            "risks": []
        }


# ============================================================
# AGE
# ============================================================

def get_age_minutes(pair):

    created = pair.get(
        "pairCreatedAt"
    )

    if not created:
        return None

    try:

        created_seconds = (
            float(created) / 1000
        )

        age_seconds = (
            time.time()
            - created_seconds
        )

        return max(
            0,
            age_seconds / 60
        )

    except Exception:

        return None


# ============================================================
# FORMATTERS
# ============================================================

def money(value):

    try:
        return f"${float(value):,.0f}"

    except Exception:
        return "$0"


def format_age(age):

    if age is None:
        return "Unknown"

    if age < 1:

        seconds = int(
            age * 60
        )

        return f"{seconds} sec"

    return f"{age:.0f} min"


# ============================================================
# RISK CLASSIFICATION
# ============================================================

def liquidity_risk(mc, liquidity):

    if mc <= 0:
        return "UNKNOWN", "⚪"

    ratio = (
        liquidity / mc
    ) * 100

    # These are scanner heuristics,
    # not guarantees of safety.

    if ratio >= 30:
        return "GOOD", "🟢"

    if ratio >= 15:
        return "MEDIUM", "🟡"

    return "LOW", "🔴"


def volume_risk(mc, volume):

    if mc <= 0:
        return "UNKNOWN", "⚪", 0

    ratio = volume / mc

    if ratio < 2:
        return "LOW", "🟡", ratio

    if ratio <= 10:
        return "NORMAL", "🟢", ratio

    if ratio <= 20:
        return "HIGH", "🟡", ratio

    return "VERY HIGH", "🔴", ratio


def holder_risk(holders):

    if holders is None:
        return "UNKNOWN", "⚪"

    if holders >= 300:
        return "GOOD", "🟢"

    if holders >= 100:
        return "MEDIUM", "🟡"

    return "LOW", "🔴"


def age_risk(age):

    if age is None:
        return "UNKNOWN", "⚪"

    if age < 5:
        return "HIGH", "🔴"

    if age < 15:
        return "MEDIUM", "🟡"

    return "MEDIUM", "🟡"


def top10_risk(top10):

    if top10 is None:
        return "UNKNOWN", "⚪"

    if top10 >= 50:
        return "HIGH", "🔴"

    if top10 >= 35:
        return "MEDIUM", "🟡"

    return "GOOD", "🟢"


# ============================================================
# OVERALL RISK
# ============================================================

def calculate_overall_risk(
    top10,
    liquidity_ratio,
    volume_ratio,
    holders,
    mint_status,
    freeze_status
):

    high_flags = 0
    medium_flags = 0

    # Top 10 concentration
    if top10 is not None:

        if top10 >= 50:
            high_flags += 1

        elif top10 >= 35:
            medium_flags += 1

    # Liquidity
    if liquidity_ratio < 15:
        high_flags += 1

    elif liquidity_ratio < 30:
        medium_flags += 1

    # Volume
    if volume_ratio > 20:
        high_flags += 1

    elif volume_ratio > 10:
        medium_flags += 1

    # Holders
    if holders is not None:

        if holders < 100:
            high_flags += 1

        elif holders < 300:
            medium_flags += 1

    # Mint authority
    if mint_status == "Active":
        high_flags += 1

    # Freeze authority
    if freeze_status == "Active":
        high_flags += 1

    if high_flags >= 1:
        return "HIGH", "🔴"

    if medium_flags >= 2:
        return "MEDIUM", "🟡"

    return "LOWER", "🟢"


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if (
        not TELEGRAM_BOT_TOKEN
        or not TELEGRAM_CHAT_ID
    ):

        log(
            "⚠️ Telegram credentials missing"
        )

        return False

    url = TELEGRAM_API.format(
        TELEGRAM_BOT_TOKEN
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }

    response = request_post(
        url,
        payload
    )

    if response is None:
        return False

    try:

        result = response.json()

        if result.get("ok"):
            return True

        log(
            f"⚠️ Telegram error: {result}"
        )

        return False

    except Exception:

        return False


# ============================================================
# BUILD ALERT
# ============================================================

def build_alert(
    name,
    symbol,
    mint_address,
    mc,
    liquidity,
    holders,
    age,
    volume,
    mint_status,
    freeze_status,
    top10,
    rug_risks
):

    # --------------------------------------------------------
    # RISK CALCULATIONS
    # --------------------------------------------------------

    if mc > 0:

        liquidity_ratio = (
            liquidity / mc
        ) * 100

        volume_ratio = (
            volume / mc
        )

    else:

        liquidity_ratio = 0
        volume_ratio = 0

    liq_label, liq_icon = (
        liquidity_risk(
            mc,
            liquidity
        )
    )

    vol_label, vol_icon, _ = (
        volume_risk(
            mc,
            volume
        )
    )

    holder_label, holder_icon = (
        holder_risk(
            holders
        )
    )

    age_label, age_icon = (
        age_risk(age)
    )

    top10_label, top10_icon = (
        top10_risk(top10)
    )

    overall_label, overall_icon = (
        calculate_overall_risk(
            top10=top10,
            liquidity_ratio=liquidity_ratio,
            volume_ratio=volume_ratio,
            holders=holders,
            mint_status=mint_status,
            freeze_status=freeze_status
        )
    )

    # --------------------------------------------------------
    # LINKS
    # --------------------------------------------------------
    #
    # IMPORTANT:
    # Use the TOKEN CA here, not the DexScreener pair address.
    #

    dex_link = (
        "https://dexscreener.com/solana/"
        + mint_address
    )

    rug_link = (
        "https://rugcheck.xyz/tokens/"
        + mint_address
    )

    pump_link = (
        "https://pump.fun/coin/"
        + mint_address
    )

    # --------------------------------------------------------
    # HOLDER TEXT
    # --------------------------------------------------------

    if holders is None:
        holder_text = "Unknown"
    else:
        holder_text = f"{holders:,}"

    # --------------------------------------------------------
    # TOP 10 TEXT
    # --------------------------------------------------------

    if top10 is None:
        top10_text = "Unknown"
    else:
        top10_text = f"{top10:.1f}%"

    # --------------------------------------------------------
    # RISK FLAGS
    # --------------------------------------------------------

    risk_lines = []

    risk_lines.append(
        f"{top10_icon} Top 10: "
        f"{top10_label} — {top10_text}"
    )

    risk_lines.append(
        f"{liq_icon} Liquidity/MC: "
        f"{liq_label} — "
        f"{liquidity_ratio:.1f}%"
    )

    risk_lines.append(
        f"{vol_icon} Volume/MC: "
        f"{vol_label} — "
        f"{volume_ratio:.1f}x"
    )

    risk_lines.append(
        f"{holder_icon} Holders: "
        f"{holder_label} — "
        f"{holder_text}"
    )

    risk_lines.append(
        f"{age_icon} Age: "
        f"{age_label} — "
        f"{format_age(age)}"
    )

    # --------------------------------------------------------
    # EXTRA RUGCHECK RISKS
    # --------------------------------------------------------

    if rug_risks:

        for risk in rug_risks[:5]:

            risk_lines.append(
                f"⚠️ RugCheck: {risk}"
            )

    # --------------------------------------------------------
    # BUILD MESSAGE
    # --------------------------------------------------------

    message = (
        "🚨 NEW SOLANA TOKEN\n\n"

        f"Name: {name} ({symbol})\n"
        f"CA: {mint_address}\n\n"

        f"💰 MC: {money(mc)}\n"
        f"💧 Liquidity: {money(liquidity)}\n"
        f"👥 Holders: {holder_text}\n"
        f"⏱ Age: {format_age(age)}\n"
        f"📊 Volume 24h: {money(volume)}\n\n"

        f"🔒 Mint: {mint_status}\n"
        f"❄️ Freeze: {freeze_status}\n"
        f"🐋 Top 10: {top10_text}\n\n"

        "🛡️ RISK CHECKS\n\n"
    )

    message += "\n".join(
        risk_lines
    )

    message += (
        "\n\n"
        f"{overall_icon} "
        f"OVERALL RISK: {overall_label}\n\n"

        "🔗 DexScreener:\n"
        f"{dex_link}\n\n"

        "🔍 RugCheck:\n"
        f"{rug_link}\n\n"

        "🚀 Pump.fun:\n"
        f"{pump_link}\n\n"

        "⚠️ DYOR — New/low-cap tokens are "
        "extremely risky. This scanner does "
        "not guarantee safety or profit."
    )

    return message


# ============================================================
# ADD NEW TOKEN
# ============================================================

def add_new_token(data):

    mint_address = (
        data.get("mint")
        or data.get("token")
        or data.get("address")
    )

    if not mint_address:
        return

    if is_seen(mint_address):
        return

    with pending_lock:

        if mint_address in pending_tokens:
            return

        if (
            len(pending_tokens)
            >= MAX_PENDING_TOKENS
        ):

            log(
                "⚠️ Pending limit reached. "
                f"Ignoring {mint_address}"
            )

            return

        now = time.time()

        pending_tokens[
            mint_address
        ] = {
            "data": data,
            "first_seen": now,
            "last_checked": 0,
            "next_check": now,
            "attempts": 0,
            "queued": False
        }

    with stats_lock:
        stats["detected"] += 1

    name = (
        data.get("name")
        or "Unknown"
    )

    symbol = (
        data.get("symbol")
        or "UNKNOWN"
    )

    dev = (
        data.get("traderPublicKey")
        or data.get("creator")
        or "Unknown"
    )

    pump_mc = data.get(
        "marketCapSol"
    )

    log(
        "🟢 NEW PUMP.FUN LAUNCH"
    )

    log(
        f"   Name: {name} ({symbol})"
    )

    log(
        f"   CA: {mint_address}"
    )

    log(
        f"   Dev: {dev}"
    )

    log(
        f"   Pump MC SOL: {pump_mc}"
    )


# ============================================================
# PENDING TOKEN
# ============================================================

def remove_pending(mint_address):

    with pending_lock:

        pending_tokens.pop(
            mint_address,
            None
        )


def reschedule_token(
    mint_address,
    delay
):

    with pending_lock:

        token = pending_tokens.get(
            mint_address
        )

        if not token:
            return

        token["next_check"] = (
            time.time() + delay
        )

        token["queued"] = False


# ============================================================
# CHECK TOKEN
# ============================================================

def check_token(mint_address):

    with pending_lock:

        token = pending_tokens.get(
            mint_address
        )

        if not token:
            return

        data = token["data"]

        first_seen = (
            token["first_seen"]
        )

        token["last_checked"] = (
            time.time()
        )

        token["attempts"] += 1

        attempt = token["attempts"]

    elapsed_minutes = (
        time.time() - first_seen
    ) / 60

    # --------------------------------------------------------
    # EXPIRE
    # --------------------------------------------------------

    if (
        elapsed_minutes
        > MONITOR_MAX_MINUTES
    ):

        log(
            f"⌛ Token expired: "
            f"{mint_address}"
        )

        remove_pending(
            mint_address
        )

        with stats_lock:
            stats["expired"] += 1

        return

    # --------------------------------------------------------
    # DEXSCREENER
    # --------------------------------------------------------

    pair = get_token_pair(
        mint_address
    )

    with stats_lock:
        stats["checked"] += 1

    if not pair:

        log(
            f"⏳ No DexScreener pair yet: "
            f"{mint_address}"
        )

        # Progressive backoff
        if attempt <= 2:
            delay = 10

        elif attempt <= 5:
            delay = 20

        elif attempt <= 10:
            delay = 30

        else:
            delay = 60

        reschedule_token(
            mint_address,
            delay
        )

        return

    # --------------------------------------------------------
    # MARKET DATA
    # --------------------------------------------------------

    try:

        mc = float(
            pair.get("marketCap")
            or pair.get("fdv")
            or 0
        )

        liquidity = float(
            (
                pair.get("liquidity")
                or {}
            ).get("usd")
            or 0
        )

        volume = float(
            (
                pair.get("volume")
                or {}
            ).get("h24")
            or 0
        )

    except Exception as e:

        log(
            f"⚠️ Market data error: "
            f"{e}"
        )

        reschedule_token(
            mint_address,
            20
        )

        return

    age = get_age_minutes(
        pair
    )

    # --------------------------------------------------------
    # AGE
    # --------------------------------------------------------

    if (
        age is not None
        and age > MONITOR_MAX_MINUTES
    ):

        log(
            f"⌛ Token too old: "
            f"{mint_address}"
        )

        remove_pending(
            mint_address
        )

        return

    # --------------------------------------------------------
    # MARKET CAP
    # --------------------------------------------------------

    if mc < MIN_MC:

        log(
            f"⏳ MC below "
            f"${MIN_MC:,}: "
            f"{mint_address} | "
            f"${mc:,.0f}"
        )

        reschedule_token(
            mint_address,
            15
        )

        return

    if mc > MAX_MC:

        log(
            f"🚫 MC above "
            f"${MAX_MC:,}: "
            f"{mint_address} | "
            f"${mc:,.0f}"
        )

        remove_pending(
            mint_address
        )

        return

    # --------------------------------------------------------
    # LIQUIDITY
    # --------------------------------------------------------

    if liquidity < MIN_LIQUIDITY:

        log(
            f"💧 Liquidity below "
            f"${MIN_LIQUIDITY:,}: "
            f"{mint_address} | "
            f"${liquidity:,.0f}"
        )

        reschedule_token(
            mint_address,
            15
        )

        return

    # --------------------------------------------------------
    # MARKET PASSED
    # --------------------------------------------------------

    log(
        f"✅ Market filters passed: "
        f"{mint_address} | "
        f"MC ${mc:,.0f} | "
        f"Liq ${liquidity:,.0f}"
    )

    # --------------------------------------------------------
    # RUGCHECK
    # --------------------------------------------------------

    rug = get_rugcheck_data(
        mint_address
    )

    holders = rug.get(
        "holders"
    )

    top10 = rug.get(
        "top10"
    )

    rug_risks = (
        rug.get("risks")
        or []
    )

    # --------------------------------------------------------
    # HOLDERS
    # --------------------------------------------------------

    if holders is None:

        log(
            f"⚠️ Holder count unavailable: "
            f"{mint_address}"
        )

        reschedule_token(
            mint_address,
            30
        )

        return

    if holders < MIN_HOLDERS:

        log(
            f"👥 Holders below "
            f"{MIN_HOLDERS}: "
            f"{mint_address} | "
            f"{holders}"
        )

        reschedule_token(
            mint_address,
            20
        )

        return

    # --------------------------------------------------------
    # AUTHORITIES
    # --------------------------------------------------------

    mint_status, freeze_status = (
        get_token_authorities(
            mint_address
        )
    )

    # --------------------------------------------------------
    # NAME / SYMBOL
    # --------------------------------------------------------

    name = (
        data.get("name")
        or (
            pair.get("baseToken", {})
            .get("name")
        )
        or "Unknown"
    )

    symbol = (
        data.get("symbol")
        or (
            pair.get("baseToken", {})
            .get("symbol")
        )
        or "UNKNOWN"
    )

    # --------------------------------------------------------
    # BUILD ALERT
    # --------------------------------------------------------

    alert = build_alert(
        name=name,
        symbol=symbol,
        mint_address=mint_address,
        mc=mc,
        liquidity=liquidity,
        holders=holders,
        age=age,
        volume=volume,
        mint_status=mint_status,
        freeze_status=freeze_status,
        top10=top10,
        rug_risks=rug_risks
    )

    # --------------------------------------------------------
    # TELEGRAM
    # --------------------------------------------------------

    success = send_telegram(
        alert
    )

    if success:

        log(
            f"🚨 TELEGRAM ALERT SENT: "
            f"{mint_address}"
        )

        mark_seen(
            mint_address
        )

        remove_pending(
            mint_address
        )

        with stats_lock:
            stats["alerts"] += 1

    else:

        log(
            f"⚠️ Telegram failed: "
            f"{mint_address}"
        )

        reschedule_token(
            mint_address,
            60
        )


# ============================================================
# WORKER
# ============================================================

def worker(worker_id):

    log(
        f"👷 Worker {worker_id} started"
    )

    while True:

        try:

            mint_address = (
                work_queue.get(
                    timeout=5
                )
            )

        except queue.Empty:

            continue

        try:

            check_token(
                mint_address
            )

        except Exception as e:

            log(
                f"❌ Worker {worker_id} "
                f"error: {e}"
            )

            reschedule_token(
                mint_address,
                30
            )

        finally:

            with pending_lock:

                token = pending_tokens.get(
                    mint_address
                )

                if token:
                    token["queued"] = False

            work_queue.task_done()


# ============================================================
# SCHEDULER
# ============================================================

def scheduler():

    log(
        "⏱ Smart token scheduler started"
    )

    while True:

        now = time.time()

        candidates = []

        with pending_lock:

            for (
                mint_address,
                token
            ) in pending_tokens.items():

                if token.get(
                    "queued"
                ):
                    continue

                if token.get(
                    "next_check",
                    0
                ) > now:

                    continue

                candidates.append(
                    mint_address
                )

        # Only add tokens when there
        # is room in the queue.

        for mint_address in candidates:

            if work_queue.full():

                with stats_lock:
                    stats["queue_full"] += 1

                break

            with pending_lock:

                token = pending_tokens.get(
                    mint_address
                )

                if not token:
                    continue

                if token.get(
                    "queued"
                ):
                    continue

                token["queued"] = True

            try:

                work_queue.put_nowait(
                    mint_address
                )

            except queue.Full:

                with pending_lock:

                    token = pending_tokens.get(
                        mint_address
                    )

                    if token:
                        token["queued"] = False

                with stats_lock:
                    stats["queue_full"] += 1

                break

        time.sleep(2)


# ============================================================
# STATUS
# ============================================================

def status_monitor():

    while True:

        time.sleep(30)

        with pending_lock:
            pending_count = len(
                pending_tokens
            )

        queue_count = (
            work_queue.qsize()
        )

        with seen_lock:
            seen_count = len(
                seen_tokens
            )

        with stats_lock:

            detected = stats[
                "detected"
            ]

            checked = stats[
                "checked"
            ]

            alerts = stats[
                "alerts"
            ]

            expired = stats[
                "expired"
            ]

            dex_429 = stats[
                "dex_429"
            ]

            queue_full = stats[
                "queue_full"
            ]

        log(
            "📊 STATUS | "
            f"Pending: {pending_count} | "
            f"Queue: {queue_count} | "
            f"Detected: {detected} | "
            f"Checked: {checked} | "
            f"Alerts: {alerts} | "
            f"Seen: {seen_count} | "
            f"Expired: {expired} | "
            f"429s: {dex_429} | "
            f"QueueFull: {queue_full}"
        )


# ============================================================
# PUMPPORTAL
# ============================================================

def on_open(ws):

    log(
        "🟢 Connected to PumpPortal"
    )

    subscription = {
        "method": "subscribeNewToken"
    }

    try:

        ws.send(
            json.dumps(
                subscription
            )
        )

        log(
            "📡 Subscribed to new "
            "Pump.fun tokens"
        )

    except Exception as e:

        log(
            f"⚠️ Subscription error: {e}"
        )


def on_message(ws, message):

    try:

        data = json.loads(
            message
        )

    except Exception:

        return

    tx_type = data.get(
        "txType"
    )

    if tx_type != "create":
        return

    add_new_token(
        data
    )


def on_error(ws, error):

    log(
        f"⚠️ PumpPortal WebSocket "
        f"error: {error}"
    )


def on_close(
    ws,
    close_status_code,
    close_msg
):

    log(
        "🔴 PumpPortal disconnected | "
        f"Code: {close_status_code} | "
        f"Message: {close_msg}"
    )


# ============================================================
# WEBSOCKET LOOP
# ============================================================

def websocket_loop():

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
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"⚠️ WebSocket exception: {e}"
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
# START WORKERS
# ============================================================

def start_workers():

    for i in range(
        1,
        WORKER_COUNT + 1
    ):

        thread = threading.Thread(
            target=worker,
            args=(i,),
            daemon=True
        )

        thread.start()


# ============================================================
# MAIN
# ============================================================

def main():

    log("=" * 60)

    log(
        "🚀 SOLANA PUMP.FUN "
        "TELEGRAM SCANNER"
    )

    log("=" * 60)

    log(
        f"💰 MC: "
        f"${MIN_MC:,} - "
        f"${MAX_MC:,}"
    )

    log(
        f"💧 Minimum liquidity: "
        f"${MIN_LIQUIDITY:,}"
    )

    log(
        f"👥 Minimum holders: "
        f"{MIN_HOLDERS}"
    )

    log(
        f"⏱ Monitor age: "
        f"{MONITOR_MAX_MINUTES} min"
    )

    log(
        f"🛡 Dex spacing: "
        f"{DEX_MIN_REQUEST_INTERVAL}s"
    )

    if TELEGRAM_BOT_TOKEN:
        log(
            "✅ Telegram bot token loaded"
        )
    else:
        log(
            "❌ Telegram bot token missing"
        )

    if TELEGRAM_CHAT_ID:
        log(
            "✅ Telegram chat ID loaded"
        )
    else:
        log(
            "❌ Telegram chat ID missing"
        )

    load_seen()

    start_workers()

    scheduler_thread = threading.Thread(
        target=scheduler,
        daemon=True
    )

    scheduler_thread.start()

    status_thread = threading.Thread(
        target=status_monitor,
        daemon=True
    )

    status_thread.start()

    log(
        "📡 Pump.fun detection: LIVE"
    )

    log(
        "🛡 429 protection: LIVE"
    )

    log(
        "📦 Smart queue: LIVE"
    )

    log(
        "🔍 Waiting for new launches..."
    )

    websocket_loop()


# ============================================================
# START
# ============================================================

if __name__ == "__main__":
    main()
