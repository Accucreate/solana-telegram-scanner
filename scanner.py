import os
import time
import json
import requests
from datetime import datetime, timezone

# ============================================================
# SETTINGS
# ============================================================

MIN_MC = 5_000
MAX_MC = 100_000
MIN_LIQUIDITY = 5_000

SCAN_INTERVAL = 300  # 5 minutes

DEXSCREENER_PROFILES = (
    "https://api.dexscreener.com/token-profiles/latest/v1"
)

DEXSCREENER_TOKEN = (
    "https://api.dexscreener.com/latest/dex/tokens/"
)

RUGCHECK_REPORT = (
    "https://api.rugcheck.xyz/v1/tokens/{}/report"
)

TELEGRAM_API = "https://api.telegram.org/bot{}/sendMessage"

SEEN_FILE = "seen_tokens.json"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ============================================================
# BASIC HELPERS
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "SolanaTelegramScanner/1.0"
})


def log(message):
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


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
        return f"{int(value):,}"
    except Exception:
        return "Unknown"


# ============================================================
# SEEN TOKENS
# ============================================================

def load_seen():

    try:
        if not os.path.exists(SEEN_FILE):
            return set()

        with open(SEEN_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)

        if isinstance(data, list):
            return set(data)

        return set()

    except Exception as e:
        log(f"Could not load seen tokens: {e}")
        return set()


def save_seen(seen):

    try:
        with open(SEEN_FILE, "w", encoding="utf-8") as file:
            json.dump(sorted(list(seen)), file, indent=2)

    except Exception as e:
        log(f"Could not save seen tokens: {e}")


# ============================================================
# DEXSCREENER
# ============================================================

def get_latest_solana_tokens():

    try:

        response = session.get(
            DEXSCREENER_PROFILES,
            timeout=20
        )

        response.raise_for_status()

        data = response.json()

        addresses = []

        if isinstance(data, list):

            for item in data:

                if item.get("chainId") != "solana":
                    continue

                address = (
                    item.get("tokenAddress")
                    or item.get("address")
                )

                if address:
                    addresses.append(address)

        return list(dict.fromkeys(addresses))

    except Exception as e:

        log(f"DexScreener discovery error: {e}")
        return []


def get_token_pair(mint):

    try:

        response = session.get(
            DEXSCREENER_TOKEN + mint,
            timeout=20
        )

        response.raise_for_status()

        data = response.json()

        pairs = data.get("pairs") or []

        solana_pairs = [
            pair
            for pair in pairs
            if pair.get("chainId") == "solana"
        ]

        if not solana_pairs:
            return None

        # Select the pair with the highest liquidity.
        solana_pairs.sort(
            key=lambda p: safe_float(
                (p.get("liquidity") or {}).get("usd")
            ),
            reverse=True
        )

        return solana_pairs[0]

    except Exception as e:

        log(f"Pair lookup error for {mint}: {e}")
        return None


# ============================================================
# RUGCHECK
# ============================================================

def get_rugcheck_data(mint):

    result = {
        "holders": None,
        "top10": None,
        "mint_authority": None,
        "freeze_authority": None,
        "risk": None
    }

    try:

        url = RUGCHECK_REPORT.format(mint)

        response = session.get(
            url,
            timeout=20
        )

        if response.status_code != 200:
            return result

        data = response.json()

        # -----------------------------
        # Holder count
        # -----------------------------

        holder_count = (
            data.get("totalHolders")
            or data.get("holderCount")
            or data.get("holdersCount")
        )

        if holder_count is not None:
            result["holders"] = holder_count

        # -----------------------------
        # Mint authority
        # -----------------------------

        mint_authority = (
            data.get("mintAuthority")
        )

        if mint_authority is None:

            token = data.get("token") or {}

            mint_authority = token.get(
                "mintAuthority"
            )

        result["mint_authority"] = mint_authority

        # -----------------------------
        # Freeze authority
        # -----------------------------

        freeze_authority = (
            data.get("freezeAuthority")
        )

        if freeze_authority is None:

            token = data.get("token") or {}

            freeze_authority = token.get(
                "freezeAuthority"
            )

        result["freeze_authority"] = freeze_authority

        # -----------------------------
        # Top 10 holder concentration
        # -----------------------------

        top_holders = data.get("topHolders")

        if isinstance(top_holders, list) and top_holders:

            total_percentage = 0

            for holder in top_holders[:10]:

                percentage = safe_float(
                    holder.get("pct")
                    or holder.get("percentage")
                    or holder.get("ownershipPercentage")
                )

                total_percentage += percentage

            if total_percentage > 0:
                result["top10"] = total_percentage

        # -----------------------------
        # Risk
        # -----------------------------

        risks = data.get("risks")

        if isinstance(risks, list):

            risk_names = []

            for risk in risks:

                if isinstance(risk, dict):

                    name = (
                        risk.get("name")
                        or risk.get("description")
                        or risk.get("level")
                    )

                    if name:
                        risk_names.append(str(name))

            if risk_names:
                result["risk"] = ", ".join(
                    risk_names[:5]
                )

        return result

    except Exception as e:

        log(f"RugCheck error for {mint}: {e}")
        return result


# ============================================================
# AUTHORITY DISPLAY
# ============================================================

def authority_status(authority):

    if authority is None:
        return "Unknown"

    if authority == "":
        return "Revoked"

    if authority is False:
        return "Revoked"

    return "Active"


# ============================================================
# TOKEN AGE
# ============================================================

def get_token_age(pair):

    try:

        created = pair.get("pairCreatedAt")

        if not created:
            return "Unknown"

        created_seconds = created / 1000

        now = datetime.now(
            timezone.utc
        ).timestamp()

        age_seconds = max(
            0,
            now - created_seconds
        )

        minutes = int(age_seconds / 60)

        if minutes < 60:
            return f"{minutes} min"

        hours = int(minutes / 60)

        if hours < 24:
            return f"{hours} hr"

        days = int(hours / 24)

        return f"{days} day"

    except Exception:
        return "Unknown"


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:
        log("ERROR: TELEGRAM_BOT_TOKEN is missing")
        return False

    if not TELEGRAM_CHAT_ID:
        log("ERROR: TELEGRAM_CHAT_ID is missing")
        return False

    url = TELEGRAM_API.format(
        TELEGRAM_BOT_TOKEN
    )

    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": False
    }

    try:

        response = session.post(
            url,
            json=payload,
            timeout=20
        )

        if response.status_code == 200:
            return True

        log(
            f"Telegram error: "
            f"{response.status_code} "
            f"{response.text[:300]}"
        )

        return False

    except Exception as e:

        log(f"Telegram connection error: {e}")
        return False


# ============================================================
# BUILD ALERT
# ============================================================

def build_alert(
    pair,
    mint,
    security
):

    base_token = pair.get("baseToken") or {}

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
        (pair.get("liquidity") or {}).get("usd")
    )

    volume = safe_float(
        (pair.get("volume") or {}).get("h24")
    )

    holders = security.get("holders")

    if holders is None:
        holders_text = "Unknown"
    else:
        holders_text = format_number(holders)

    top10 = security.get("top10")

    if top10 is None:
        top10_text = "Unknown"
    else:
        top10_text = f"{top10:.1f}%"

    mint_status = authority_status(
        security.get("mint_authority")
    )

    freeze_status = authority_status(
        security.get("freeze_authority")
    )

    age = get_token_age(pair)

    dex_url = (
        pair.get("url")
        or f"https://dexscreener.com/solana/{mint}"
    )

    rug_url = (
        f"https://rugcheck.xyz/tokens/{mint}"
    )

    message = f"""🚨 NEW SOLANA TOKEN

Name: {name} ({symbol})

CA:
{mint}

💰 MC: {format_money(mc)}
💧 Liquidity: {format_money(liquidity)}
👥 Holders: {holders_text}
⏱ Age: {age}
📊 Volume 24h: {format_money(volume)}

🔒 Mint: {mint_status}
❄️ Freeze: {freeze_status}
🐋 Top 10: {top10_text}

🔗 DexScreener:
{dex_url}

🔍 RugCheck:
{rug_url}

⚠️ DYOR — New/low-cap tokens are extremely risky.
"""

    return message


# ============================================================
# SCAN
# ============================================================

def scan():

    seen = load_seen()

    log("=" * 60)
    log("🚀 SOLANA SCANNER STARTED")
    log(
        f"Filters: MC ${MIN_MC:,} - "
        f"${MAX_MC:,} | "
        f"Liquidity >= ${MIN_LIQUIDITY:,}"
    )
    log(
        f"Previously alerted tokens: "
        f"{len(seen)}"
    )

    addresses = get_latest_solana_tokens()

    log(
        f"Discovered {len(addresses)} "
        f"Solana token addresses"
    )

    checked = 0
    alerts = 0

    for mint in addresses:

        if mint in seen:
            continue

        checked += 1

        pair = get_token_pair(mint)

        if not pair:
            continue

        mc = safe_float(
            pair.get("marketCap")
            or pair.get("fdv")
        )

        liquidity = safe_float(
            (pair.get("liquidity") or {}).get("usd")
        )

        # -----------------------------
        # MARKET CAP FILTER
        # -----------------------------

        if mc < MIN_MC:
            continue

        if mc > MAX_MC:
            continue

        # -----------------------------
        # LIQUIDITY FILTER
        # -----------------------------

        if liquidity < MIN_LIQUIDITY:
            continue

        log(
            f"QUALIFIED: {mint} | "
            f"MC={format_money(mc)} | "
            f"Liquidity={format_money(liquidity)}"
        )

        security = get_rugcheck_data(mint)

        message = build_alert(
            pair,
            mint,
            security
        )

        if send_telegram(message):

            log(
                f"Telegram alert sent: {mint}"
            )

            seen.add(mint)
            alerts += 1

            save_seen(seen)

        else:

            log(
                f"Telegram failed: {mint}"
            )

    save_seen(seen)

    log("=" * 60)
    log(
        f"SCAN COMPLETE | "
        f"Checked: {checked} | "
        f"New alerts: {alerts} | "
        f"Seen: {len(seen)}"
    )
    log("=" * 60)


# ============================================================
# MAIN LOOP
# ============================================================

def main():

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

    log("Telegram credentials detected.")
    log("Automatic scanner is ready.")
    log(
        f"Scanner will run every "
        f"{SCAN_INTERVAL // 60} minutes."
    )

    while True:

        try:
            scan()

        except Exception as e:

            log(
                f"Unexpected scanner error: {e}"
            )

        log(
            f"Sleeping for "
            f"{SCAN_INTERVAL} seconds..."
        )

        time.sleep(SCAN_INTERVAL)


if __name__ == "__main__":
    main()
