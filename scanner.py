import os
import json
import time
import threading
import queue
from datetime import datetime, timezone

import requests
import websocket


# ============================================================
# CONFIGURATION
# ============================================================

MIN_MC = 5_000
MAX_MC = 100_000
MIN_LIQUIDITY = 5_000
MIN_HOLDERS = 100

# Stop watching a token after this many minutes
MONITOR_MAX_MINUTES = 30

# Maximum number of tokens waiting to be processed
MAX_PENDING_TOKENS = 500

# Queue is intentionally small.
# We do NOT want hundreds of duplicate jobs.
MAX_QUEUE_SIZE = 30

# Number of worker threads.
# Keep this low because external APIs have rate limits.
WORKER_COUNT = 2

# Minimum time between DexScreener requests.
# This protects against 429 errors.
DEX_MIN_REQUEST_INTERVAL = 1.5

# Timeouts
REQUEST_TIMEOUT = 20
WS_TIMEOUT = 30

# Retry settings for normal errors
MAX_RETRIES = 2
RETRY_DELAY = 2

# PumpPortal WebSocket
PUMPPORTAL_WS = "wss://pumpportal.fun/api/data"

# Solana RPC
SOLANA_RPC = "https://api.mainnet-beta.solana.com"

# DexScreener
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/"

# RugCheck
RUGCHECK_REPORT = "https://api.rugcheck.xyz/v1/tokens/{}/report"

# Telegram
TELEGRAM_API = "https://api.telegram.org/bot{}/sendMessage"

# Seen tokens file
SEEN_FILE = "seen_tokens.json"


# ============================================================
# TELEGRAM SETTINGS
# ============================================================

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ============================================================
# GLOBAL OBJECTS
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "Mozilla/5.0 Solana-Token-Scanner/2.0"
})

work_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)

pending_lock = threading.Lock()
seen_lock = threading.Lock()

pending_tokens = {}
seen_tokens = set()

# Protects DexScreener from excessive requests
dex_lock = threading.Lock()
last_dex_request = 0.0

# Statistics
stats_lock = threading.Lock()

stats = {
    "detected": 0,
    "checked": 0,
    "alerts": 0,
    "expired": 0,
    "dex_429": 0,
    "queue_full": 0,
}


# ============================================================
# LOGGING
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

        log(f"📂 Loaded {len(seen_tokens)} seen tokens")

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
# HTTP REQUEST HELPERS
# ============================================================

def request_get(url, params=None, headers=None):
    """
    Normal GET request.

    IMPORTANT:
    429 is NOT aggressively retried.
    This is one of the main fixes for the previous problem.
    """

    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.get(
                url,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT
            )

            # ------------------------------------------------
            # RATE LIMIT
            # ------------------------------------------------

            if response.status_code == 429:

                retry_after = response.headers.get("Retry-After")

                try:
                    wait_time = float(retry_after)
                except (TypeError, ValueError):
                    wait_time = 10

                with stats_lock:
                    stats["dex_429"] += 1

                log(
                    f"🛑 API rate limit (429). "
                    f"Waiting {wait_time:.1f}s instead of retrying rapidly."
                )

                time.sleep(min(wait_time, 30))

                # Do not immediately hammer the API again
                return None

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            if attempt >= MAX_RETRIES:
                log(
                    f"⚠️ GET failed after {MAX_RETRIES} attempts: "
                    f"{url}"
                )
                return None

            wait_time = RETRY_DELAY * attempt

            log(
                f"⚠️ GET attempt {attempt} failed. "
                f"Retrying in {wait_time}s..."
            )

            time.sleep(wait_time)

        except Exception as e:

            log(f"⚠️ Unexpected GET error: {e}")
            return None

    return None


def request_post(url, json_data=None):
    for attempt in range(1, MAX_RETRIES + 1):

        try:
            response = session.post(
                url,
                json=json_data,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 429:

                retry_after = response.headers.get("Retry-After")

                try:
                    wait_time = float(retry_after)
                except (TypeError, ValueError):
                    wait_time = 10

                log(
                    f"🛑 POST rate limited. "
                    f"Waiting {wait_time:.1f}s."
                )

                time.sleep(min(wait_time, 30))
                return None

            response.raise_for_status()

            return response

        except requests.RequestException:

            if attempt >= MAX_RETRIES:
                return None

            time.sleep(RETRY_DELAY * attempt)

        except Exception as e:

            log(f"⚠️ POST error: {e}")
            return None

    return None


# ============================================================
# DEXSCREENER RATE LIMITER
# ============================================================

def dex_rate_limit():
    """
    Guarantees a minimum delay between DexScreener requests.

    This prevents several worker threads from simultaneously
    hitting DexScreener.
    """

    global last_dex_request

    with dex_lock:

        now = time.time()

        elapsed = now - last_dex_request

        if elapsed < DEX_MIN_REQUEST_INTERVAL:

            wait_time = DEX_MIN_REQUEST_INTERVAL - elapsed

            time.sleep(wait_time)

        last_dex_request = time.time()


# ============================================================
# DEXSCREENER
# ============================================================

def get_token_pair(mint_address):
    """
    Gets the best Solana pair from DexScreener.

    Returns:
        pair dictionary
        None if not indexed yet / failed
    """

    dex_rate_limit()

    url = DEXSCREENER_TOKEN + mint_address

    response = request_get(url)

    if response is None:
        return None

    try:
        data = response.json()

        pairs = data.get("pairs") or []

        solana_pairs = [
            p for p in pairs
            if p.get("chainId") == "solana"
        ]

        if not solana_pairs:
            return None

        # Pick highest liquidity pair
        best_pair = max(
            solana_pairs,
            key=lambda p: float(
                (p.get("liquidity") or {}).get("usd") or 0
            )
        )

        return best_pair

    except Exception as e:
        log(f"⚠️ DexScreener JSON error: {e}")
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
    """
    Checks mint authority and freeze authority.
    """

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

        result = data.get("result", {}).get("value")

        if not result:
            return "Unknown", "Unknown"

        parsed = (
            result
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )

        mint_authority = parsed.get("mintAuthority")
        freeze_authority = parsed.get("freezeAuthority")

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

        return mint_status, freeze_status

    except Exception as e:

        log(
            f"⚠️ Authority check failed for "
            f"{mint_address}: {e}"
        )

        return "Unknown", "Unknown"


# ============================================================
# RUGCHECK
# ============================================================

def get_rugcheck_data(mint_address):
    """
    Gets holder count and top-10 concentration.

    RugCheck response formats can change, so this function
    checks several possible fields.
    """

    url = RUGCHECK_REPORT.format(mint_address)

    response = request_get(url)

    if response is None:
        return {
            "holders": None,
            "top10": None,
            "risks": []
        }

    try:

        data = response.json()

        # -------------------------------
        # HOLDERS
        # -------------------------------

        holders = data.get("totalHolders")

        if holders is None:
            holders = data.get("holdersCount")

        if holders is None:
            holders = data.get("holderCount")

        try:
            holders = int(holders) if holders is not None else None
        except Exception:
            holders = None

        # -------------------------------
        # TOP 10
        # -------------------------------

        top10 = None

        # Possible direct fields
        possible_top10 = [
            data.get("top10"),
            data.get("top10Holders"),
            data.get("topHoldersPercentage"),
        ]

        for value in possible_top10:

            if value is not None:

                try:
                    top10 = float(value)

                    if top10 <= 1:
                        top10 *= 100

                    break

                except Exception:
                    pass

        # Try topHolders array
        if top10 is None:

            top_holders = data.get("topHolders")

            if isinstance(top_holders, list):

                total = 0.0

                for holder in top_holders[:10]:

                    pct = (
                        holder.get("pct")
                        if isinstance(holder, dict)
                        else None
                    )

                    if pct is None and isinstance(holder, dict):
                        pct = holder.get("percentage")

                    try:
                        pct = float(pct)

                        if pct <= 1:
                            pct *= 100

                        total += pct

                    except Exception:
                        continue

                if total > 0:
                    top10 = total

        # -------------------------------
        # RISKS
        # -------------------------------

        risks = []

        raw_risks = data.get("risks")

        if isinstance(raw_risks, list):

            for risk in raw_risks[:10]:

                if isinstance(risk, dict):

                    name = risk.get("name") or risk.get("description")

                    if name:
                        risks.append(str(name))

                else:
                    risks.append(str(risk))

        return {
            "holders": holders,
            "top10": top10,
            "risks": risks
        }

    except Exception as e:

        log(f"⚠️ RugCheck error: {e}")

        return {
            "holders": None,
            "top10": None,
            "risks": []
        }


# ============================================================
# AGE
# ============================================================

def get_age_minutes(pair):
    created = pair.get("pairCreatedAt")

    if not created:
        return None

    try:

        # DexScreener normally gives milliseconds
        created_seconds = float(created) / 1000

        age_seconds = time.time() - created_seconds

        return max(0, age_seconds / 60)

    except Exception:
        return None


# ============================================================
# FORMATTING
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
        return f"{int(age * 60)} sec"

    return f"{age:.1f} min"


# ============================================================
# RISK FLAGS
# ============================================================

def build_risk_flags(
    mc,
    liquidity,
    holders,
    top10,
    age,
    volume
):

    flags = []

    if top10 is not None:

        if top10 >= 50:
            flags.append("⚠️ Top 10 concentration > 50%")

        elif top10 >= 35:
            flags.append("⚠️ Top 10 concentration > 35%")

    if mc and liquidity:

        ratio = liquidity / mc

        if ratio < 0.05:
            flags.append("⚠️ Low liquidity/MC ratio")

    if mc and volume:

        volume_ratio = volume / mc

        if volume_ratio > 20:
            flags.append("⚠️ Unusually high volume/MC")

    if holders is not None and holders < 100:
        flags.append("⚠️ Low holder count")

    if age is not None and age < 2:
        flags.append("⚠️ Very new token")

    return flags


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:

        log(
            "⚠️ Telegram credentials missing. "
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
        )

        return False

    url = TELEGRAM_API.format(TELEGRAM_BOT_TOKEN)

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

        data = response.json()

        if data.get("ok"):
            return True

        log(f"⚠️ Telegram rejected message: {data}")
        return False

    except Exception:
        return False


# ============================================================
# TELEGRAM ALERT
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
    risks
):

    dex_link = (
        f"https://dexscreener.com/solana/"
        f"{mint_address}"
    )

    rug_link = (
        f"https://rugcheck.xyz/tokens/"
        f"{mint_address}"
    )

    pump_link = (
        f"https://pump.fun/coin/"
        f"{mint_address}"
    )

    if holders is None:
        holder_text = "Unknown"
    else:
        holder_text = f"{holders:,}"

    if top10 is None:
        top10_text = "Unknown"
    else:
        top10_text = f"{top10:.1f}%"

    message = (
        "🚨 NEW SOLANA TOKEN\n\n"
        f"Name: {name}\n"
        f"Symbol: ${symbol}\n"
        f"CA: `{mint_address}`\n\n"
        f"💰 MC: {money(mc)}\n"
        f"💧 Liquidity: {money(liquidity)}\n"
        f"👥 Holders: {holder_text}\n"
        f"⏱ Age: {format_age(age)}\n"
        f"📊 Volume: {money(volume)}\n\n"
        f"🔒 Mint: {mint_status}\n"
        f"❄️ Freeze: {freeze_status}\n"
        f"🐋 Top 10: {top10_text}\n"
    )

    if risks:

        message += "\n⚠️ Risk Flags:\n"

        for risk in risks[:6]:
            message += f"• {risk}\n"

    message += (
        "\n🔗 DexScreener:\n"
        f"{dex_link}\n\n"
        "🔍 RugCheck:\n"
        f"{rug_link}\n\n"
        "🚀 Pump.fun:\n"
        f"{pump_link}\n\n"
        "⚠️ DYOR. This is only a scanner alert, "
        "not financial advice."
    )

    return message


# ============================================================
# TOKEN SCHEDULING
# ============================================================

def add_new_token(data):
    """
    Adds a newly discovered Pump.fun token.

    IMPORTANT:
    We only add it once.
    """

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

        if len(pending_tokens) >= MAX_PENDING_TOKENS:

            log(
                f"⚠️ Pending token limit reached. "
                f"Ignoring new token: {mint_address}"
            )

            return

        now = time.time()

        pending_tokens[mint_address] = {
            "data": data,
            "first_seen": now,
            "last_checked": 0,
            "next_check": now,
            "attempts": 0,
            "queued": False
        }

    with stats_lock:
        stats["detected"] += 1

    name = data.get("name") or "Unknown"
    symbol = data.get("symbol") or "UNKNOWN"
    dev = data.get("traderPublicKey") or data.get("creator") or "Unknown"
    pump_mc = data.get("marketCapSol")

    log("🟢 NEW PUMP.FUN LAUNCH")
    log(f"   Name: {name} ({symbol})")
    log(f"   CA: {mint_address}")
    log(f"   Dev: {dev}")
    log(f"   Pump MC SOL: {pump_mc}")


# ============================================================
# REMOVE TOKEN FROM PENDING
# ============================================================

def remove_pending(mint_address):

    with pending_lock:
        pending_tokens.pop(mint_address, None)


# ============================================================
# RESCHEDULE
# ============================================================

def reschedule_token(mint_address, delay):

    with pending_lock:

        token = pending_tokens.get(mint_address)

        if not token:
            return

        token["next_check"] = time.time() + delay
        token["queued"] = False


# ============================================================
# CHECK TOKEN
# ============================================================

def check_token(mint_address):

    with pending_lock:

        token = pending_tokens.get(mint_address)

        if not token:
            return

        data = token["data"]
        first_seen = token["first_seen"]

        token["last_checked"] = time.time()
        token["attempts"] += 1

        attempt = token["attempts"]

    elapsed_minutes = (
        time.time() - first_seen
    ) / 60

    # --------------------------------------------------------
    # EXPIRE TOKEN
    # --------------------------------------------------------

    if elapsed_minutes > MONITOR_MAX_MINUTES:

        log(
            f"⌛ Token expired: {mint_address}"
        )

        remove_pending(mint_address)

        with stats_lock:
            stats["expired"] += 1

        return

    # --------------------------------------------------------
    # DEXSCREENER
    # --------------------------------------------------------

    pair = get_token_pair(mint_address)

    with stats_lock:
        stats["checked"] += 1

    # No DexScreener pair yet
    if not pair:

        log(
            f"⏳ No DexScreener pair yet: "
            f"{mint_address}"
        )

        # Gradually increase wait time
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
    # GET MARKET DATA
    # --------------------------------------------------------

    try:

        mc = float(
            pair.get("marketCap")
            or pair.get("fdv")
            or 0
        )

        liquidity = float(
            (pair.get("liquidity") or {}).get("usd")
            or 0
        )

        volume = float(
            (pair.get("volume") or {}).get("h24")
            or 0
        )

    except Exception as e:

        log(
            f"⚠️ Market data error "
            f"{mint_address}: {e}"
        )

        reschedule_token(
            mint_address,
            20
        )

        return

    age = get_age_minutes(pair)

    # --------------------------------------------------------
    # AGE
    # --------------------------------------------------------

    if age is not None and age > MONITOR_MAX_MINUTES:

        log(
            f"⌛ Token too old: "
            f"{mint_address} | "
            f"{age:.1f} min"
        )

        remove_pending(mint_address)

        return

    # --------------------------------------------------------
    # MC TOO LOW
    # --------------------------------------------------------

    if mc < MIN_MC:

        log(
            f"⏳ MC below ${MIN_MC:,}: "
            f"{mint_address} | "
            f"${mc:,.0f}"
        )

        reschedule_token(
            mint_address,
            15
        )

        return

    # --------------------------------------------------------
    # MC TOO HIGH
    # --------------------------------------------------------

    if mc > MAX_MC:

        log(
            f"🚫 MC above ${MAX_MC:,}: "
            f"{mint_address} | "
            f"${mc:,.0f}"
        )

        remove_pending(mint_address)

        return

    # --------------------------------------------------------
    # LIQUIDITY TOO LOW
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
    # MARKET CONDITIONS PASSED
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

    rug = get_rugcheck_data(mint_address)

    holders = rug.get("holders")
    top10 = rug.get("top10")
    rug_risks = rug.get("risks") or []

    # --------------------------------------------------------
    # HOLDER FILTER
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
    # SECURITY CHECKS
    # --------------------------------------------------------

    mint_status, freeze_status = (
        get_token_authorities(mint_address)
    )

    # --------------------------------------------------------
    # TOKEN NAME
    # --------------------------------------------------------

    name = (
        data.get("name")
        or pair.get("baseToken", {}).get("name")
        or "Unknown"
    )

    symbol = (
        data.get("symbol")
        or pair.get("baseToken", {}).get("symbol")
        or "UNKNOWN"
    )

    # --------------------------------------------------------
    # RISK FLAGS
    # --------------------------------------------------------

    risk_flags = build_risk_flags(
        mc=mc,
        liquidity=liquidity,
        holders=holders,
        top10=top10,
        age=age,
        volume=volume
    )

    # Add RugCheck risks
    for risk in rug_risks:

        text = str(risk)

        if text not in risk_flags:
            risk_flags.append(
                f"RugCheck: {text}"
            )

    # --------------------------------------------------------
    # ALERT
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
        risks=risk_flags
    )

    success = send_telegram(alert)

    if success:

        log(
            f"🚨 TELEGRAM ALERT SENT: "
            f"{mint_address}"
        )

        mark_seen(mint_address)

        remove_pending(mint_address)

        with stats_lock:
            stats["alerts"] += 1

    else:

        log(
            f"⚠️ Telegram alert failed: "
            f"{mint_address}"
        )

        # Keep it pending in case Telegram/API
        # temporarily failed.
        reschedule_token(
            mint_address,
            60
        )


# ============================================================
# WORKER
# ============================================================

def worker(worker_id):

    log(f"👷 Worker {worker_id} started")

    while True:

        try:

            mint_address = work_queue.get(
                timeout=5
            )

        except queue.Empty:
            continue

        try:

            check_token(mint_address)

        except Exception as e:

            log(
                f"❌ Worker {worker_id} error "
                f"on {mint_address}: {e}"
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

    log("⏱ Token scheduler started")

    while True:

        now = time.time()

        candidates = []

        with pending_lock:

            for mint_address, token in pending_tokens.items():

                if token.get("queued"):
                    continue

                if token.get("next_check", 0) > now:
                    continue

                candidates.append(
                    mint_address
                )

        # Queue only a small number at a time
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

                if token.get("queued"):
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
# STATUS MONITOR
# ============================================================

def status_monitor():

    while True:

        time.sleep(30)

        with pending_lock:
            pending_count = len(
                pending_tokens
            )

        queue_count = work_queue.qsize()

        with seen_lock:
            seen_count = len(
                seen_tokens
            )

        with stats_lock:

            detected = stats["detected"]
            checked = stats["checked"]
            alerts = stats["alerts"]
            expired = stats["expired"]
            dex_429 = stats["dex_429"]
            queue_full = stats["queue_full"]

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
# PUMPPORTAL WEBSOCKET
# ============================================================

def on_open(ws):

    log("🟢 Connected to PumpPortal")

    subscription = {
        "method": "subscribeNewToken"
    }

    try:

        ws.send(
            json.dumps(subscription)
        )

        log(
            "📡 Subscribed to new Pump.fun tokens"
        )

    except Exception as e:

        log(
            f"⚠️ Subscription error: {e}"
        )


def on_message(ws, message):

    try:

        data = json.loads(message)

    except Exception:

        return

    # PumpPortal sends different message types.
    # We only care about token creation events.

    tx_type = data.get("txType")

    if tx_type != "create":
        return

    add_new_token(data)


def on_error(ws, error):

    log(
        f"⚠️ PumpPortal WebSocket error: "
        f"{error}"
    )


def on_close(ws, close_status_code, close_msg):

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

        time.sleep(reconnect_delay)

        reconnect_delay = min(
            reconnect_delay * 2,
            60
        )


# ============================================================
# START WORKERS
# ============================================================

def start_workers():

    for i in range(1, WORKER_COUNT + 1):

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
    log("🚀 SOLANA PUMP.FUN TELEGRAM SCANNER")
    log("=" * 60)

    log(
        f"💰 MC range: "
        f"${MIN_MC:,} - ${MAX_MC:,}"
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
        f"⏱ Maximum monitoring age: "
        f"{MONITOR_MAX_MINUTES} minutes"
    )

    log(
        f"🛡 Dex request spacing: "
        f"{DEX_MIN_REQUEST_INTERVAL}s"
    )

    if TELEGRAM_BOT_TOKEN:
        log("✅ Telegram bot token loaded")
    else:
        log("❌ Telegram bot token NOT found")

    if TELEGRAM_CHAT_ID:
        log("✅ Telegram chat ID loaded")
    else:
        log("❌ Telegram chat ID NOT found")

    load_seen()

    # Start workers
    start_workers()

    # Start scheduler
    scheduler_thread = threading.Thread(
        target=scheduler,
        daemon=True
    )

    scheduler_thread.start()

    # Start status monitor
    status_thread = threading.Thread(
        target=status_monitor,
        daemon=True
    )

    status_thread.start()

    log(
        "📡 Pump.fun detection is LIVE"
    )

    log(
        "🛡 DexScreener rate protection is LIVE"
    )

    log(
        "📦 Smart queue scheduling is LIVE"
    )

    log(
        "🔍 Waiting for new Pump.fun launches..."
    )

    # Main WebSocket loop
    websocket_loop()


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    main()
