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

SOLANA_RPC = (
    "https://api.mainnet-beta.solana.com"
)

TELEGRAM_API = (
    "https://api.telegram.org/bot{}/sendMessage"
)

SEEN_FILE = "seen_tokens.json"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


# ============================================================
# RISK SETTINGS
# ============================================================

# TOP 10 CONCENTRATION
TOP10_GREEN = 30
TOP10_YELLOW = 45

# LIQUIDITY / MARKET CAP
LIQ_MC_GREEN = 30
LIQ_MC_YELLOW = 15

# VOLUME / MARKET CAP
VOL_MC_GREEN_MAX = 5
VOL_MC_YELLOW_MAX = 10

# HOLDERS
HOLDERS_GREEN = 500
HOLDERS_YELLOW = 100

# TOKEN AGE
AGE_GREEN = 60
AGE_YELLOW = 15


# ============================================================
# SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "SolanaTelegramScanner/3.0"
})


# ============================================================
# BASIC HELPERS
# ============================================================

def log(message):

    now = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(
        f"[{now}] {message}",
        flush=True
    )


def safe_float(value, default=0):

    try:

        if value is None:
            return default

        if isinstance(value, str):

            value = value.replace(",", "")
            value = value.replace("$", "")
            value = value.replace("%", "")

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

        with open(
            SEEN_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        if isinstance(data, list):
            return set(data)

        return set()

    except Exception as e:

        log(
            f"Could not load seen tokens: {e}"
        )

        return set()


def save_seen(seen):

    try:

        with open(
            SEEN_FILE,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                sorted(list(seen)),
                file,
                indent=2
            )

    except Exception as e:

        log(
            f"Could not save seen tokens: {e}"
        )


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

        return list(
            dict.fromkeys(addresses)
        )

    except Exception as e:

        log(
            f"DexScreener discovery error: {e}"
        )

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

        solana_pairs.sort(
            key=lambda p: safe_float(
                (p.get("liquidity") or {}).get("usd")
            ),
            reverse=True
        )

        return solana_pairs[0]

    except Exception as e:

        log(
            f"Pair lookup error for {mint}: {e}"
        )

        return None


# ============================================================
# SOLANA ON-CHAIN AUTHORITY CHECK
# ============================================================

def get_token_authorities(mint):

    result = {
        "mint_authority": None,
        "freeze_authority": None,
        "success": False
    }

    try:

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

        response = session.post(
            SOLANA_RPC,
            json=payload,
            timeout=20
        )

        response.raise_for_status()

        data = response.json()

        value = (
            data
            .get("result", {})
            .get("value")
        )

        if not value:
            return result

        account_data = value.get("data") or {}

        parsed = account_data.get("parsed") or {}

        info = parsed.get("info") or {}

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
            f"On-chain authority error "
            f"for {mint}: {e}"
        )

        return result


# ============================================================
# RUGCHECK
# ============================================================

def get_rugcheck_data(mint):

    result = {
        "holders": None,
        "top10": None,
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
        # HOLDERS
        # -----------------------------

        holder_count = (
            data.get("totalHolders")
            or data.get("holderCount")
            or data.get("holdersCount")
        )

        if holder_count is not None:
            result["holders"] = holder_count

        # -----------------------------
        # TOP 10
        # -----------------------------

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

                # Some APIs return percentage
                # as decimal (0.25 = 25%).
                # Convert decimal format.
                if 0 < percentage <= 1:
                    percentage *= 100

                total_percentage += percentage

            if total_percentage > 0:
                result["top10"] = (
                    total_percentage
                )

        # -----------------------------
        # RISKS
        # -----------------------------

        risks = data.get("risks")

        if isinstance(risks, list):

            risk_names = []

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
                    risk_names.append(
                        str(name)
                    )

            if risk_names:

                result["risk"] = ", ".join(
                    risk_names[:5]
                )

        return result

    except Exception as e:

        log(
            f"RugCheck error for {mint}: {e}"
        )

        return result


# ============================================================
# AUTHORITY DISPLAY
# ============================================================

def authority_status(authority):

    if authority is None:
        return "Revoked"

    if authority == "":
        return "Revoked"

    return "Active"


# ============================================================
# TOKEN AGE
# ============================================================

def get_age_minutes(pair):

    try:

        created = pair.get(
            "pairCreatedAt"
        )

        if not created:
            return None

        created_seconds = (
            safe_float(created) / 1000
        )

        now = datetime.now(
            timezone.utc
        ).timestamp()

        age_seconds = max(
            0,
            now - created_seconds
        )

        return age_seconds / 60

    except Exception:

        return None


def format_age(pair):

    minutes = get_age_minutes(pair)

    if minutes is None:
        return "Unknown"

    minutes_int = int(minutes)

    if minutes_int < 60:
        return f"{minutes_int} min"

    hours = int(minutes_int / 60)

    if hours < 24:
        return f"{hours} hr"

    days = int(hours / 24)

    return f"{days} day"


# ============================================================
# RISK HELPERS
# ============================================================

def risk_emoji(level):

    if level == "LOW":
        return "🟢"

    if level == "MEDIUM":
        return "🟡"

    if level == "HIGH":
        return "🔴"

    return "⚪"


# ------------------------------------------------------------
# TOP 10 RISK
# ------------------------------------------------------------

def evaluate_top10(top10):

    if top10 is None:
        return {
            "level": "UNKNOWN",
            "text": "Unknown"
        }

    if top10 <= TOP10_GREEN:
        return {
            "level": "LOW",
            "text": f"{top10:.1f}%"
        }

    if top10 <= TOP10_YELLOW:
        return {
            "level": "MEDIUM",
            "text": f"{top10:.1f}%"
        }

    return {
        "level": "HIGH",
        "text": f"{top10:.1f}%"
    }


# ------------------------------------------------------------
# LIQUIDITY / MC
# ------------------------------------------------------------

def evaluate_liquidity_mc(liquidity, mc):

    if mc <= 0:
        return {
            "level": "UNKNOWN",
            "text": "Unknown"
        }

    ratio = (
        liquidity / mc
    ) * 100

    if ratio >= LIQ_MC_GREEN:
        level = "LOW"

    elif ratio >= LIQ_MC_YELLOW:
        level = "MEDIUM"

    else:
        level = "HIGH"

    return {
        "level": level,
        "text": f"{ratio:.1f}%"
    }


# ------------------------------------------------------------
# VOLUME / MC
# ------------------------------------------------------------

def evaluate_volume_mc(volume, mc):

    if mc <= 0:
        return {
            "level": "UNKNOWN",
            "text": "Unknown"
        }

    ratio = volume / mc

    if ratio <= VOL_MC_GREEN_MAX:
        level = "LOW"

    elif ratio <= VOL_MC_YELLOW_MAX:
        level = "MEDIUM"

    else:
        level = "HIGH"

    return {
        "level": level,
        "text": f"{ratio:.1f}x"
    }


# ------------------------------------------------------------
# HOLDERS
# ------------------------------------------------------------

def evaluate_holders(holders):

    if holders is None:
        return {
            "level": "UNKNOWN",
            "text": "Unknown"
        }

    holders_value = safe_float(
        holders
    )

    if holders_value >= HOLDERS_GREEN:
        level = "LOW"

    elif holders_value >= HOLDERS_YELLOW:
        level = "MEDIUM"

    else:
        level = "HIGH"

    return {
        "level": level,
        "text": format_number(holders_value)
    }


# ------------------------------------------------------------
# AGE
# ------------------------------------------------------------

def evaluate_age(age_minutes):

    if age_minutes is None:
        return {
            "level": "UNKNOWN",
            "text": "Unknown"
        }

    if age_minutes >= AGE_GREEN:
        level = "LOW"

    elif age_minutes >= AGE_YELLOW:
        level = "MEDIUM"

    else:
        level = "HIGH"

    if age_minutes < 60:
        text = f"{int(age_minutes)} min"

    else:
        text = f"{int(age_minutes / 60)} hr"

    return {
        "level": level,
        "text": text
    }


# ============================================================
# OVERALL RISK
# ============================================================

def calculate_overall_risk(flags):

    levels = []

    for flag in flags:

        level = flag.get("level")

        if level in (
            "LOW",
            "MEDIUM",
            "HIGH"
        ):
            levels.append(level)

    if not levels:
        return "UNKNOWN"

    high_count = levels.count("HIGH")
    medium_count = levels.count("MEDIUM")

    # Any two or more major warning signals
    # produces HIGH overall risk.
    if high_count >= 2:
        return "HIGH"

    if high_count == 1 and medium_count >= 1:
        return "HIGH"

    if high_count == 1:
        return "MEDIUM"

    if medium_count >= 2:
        return "MEDIUM"

    if medium_count == 1:
        return "MEDIUM"

    return "LOW"


# ============================================================
# BUILD RISK FLAGS
# ============================================================

def build_risk_flags(
    mc,
    liquidity,
    volume,
    holders,
    top10,
    age_minutes
):

    top10_result = evaluate_top10(
        top10
    )

    liquidity_result = evaluate_liquidity_mc(
        liquidity,
        mc
    )

    volume_result = evaluate_volume_mc(
        volume,
        mc
    )

    holders_result = evaluate_holders(
        holders
    )

    age_result = evaluate_age(
        age_minutes
    )

    flags = [
        {
            "name": "Top 10 concentration",
            **top10_result
        },
        {
            "name": "Liquidity/MC",
            **liquidity_result
        },
        {
            "name": "Volume/MC",
            **volume_result
        },
        {
            "name": "Holders",
            **holders_result
        },
        {
            "name": "Age",
            **age_result
        }
    ]

    overall = calculate_overall_risk(
        flags
    )

    return flags, overall


# ============================================================
# TELEGRAM
# ============================================================

def send_telegram(message):

    if not TELEGRAM_BOT_TOKEN:

        log(
            "ERROR: TELEGRAM_BOT_TOKEN "
            "is missing"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        log(
            "ERROR: TELEGRAM_CHAT_ID "
            "is missing"
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

        log(
            f"Telegram connection error: {e}"
        )

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
        pair.get("baseToken") or {}
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
        (pair.get("liquidity") or {})
        .get("usd")
    )

    volume = safe_float(
        (pair.get("volume") or {})
        .get("h24")
    )

    # -----------------------------
    # HOLDERS
    # -----------------------------

    holders = security.get(
        "holders"
    )

    if holders is None:
        holders_text = "Unknown"

    else:
        holders_text = format_number(
            holders
        )

    # -----------------------------
    # TOP 10
    # -----------------------------

    top10 = security.get(
        "top10"
    )

    if top10 is None:
        top10_text = "Unknown"

    else:
        top10_text = (
            f"{top10:.1f}%"
        )

    # -----------------------------
    # AUTHORITIES
    # -----------------------------

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

    # -----------------------------
    # AGE
    # -----------------------------

    age_minutes = get_age_minutes(
        pair
    )

    age = format_age(pair)

    # -----------------------------
    # RISK FLAGS
    # -----------------------------

    flags, overall = build_risk_flags(
        mc,
        liquidity,
        volume,
        holders,
        top10,
        age_minutes
    )

    # -----------------------------
    # RISK MESSAGE
    # -----------------------------

    risk_lines = []

    for flag in flags:

        emoji = risk_emoji(
            flag["level"]
        )

        risk_lines.append(
            f"{emoji} "
            f"{flag['name']}: "
            f"{flag['level']} — "
            f"{flag['text']}"
        )

    risk_text = "\n".join(
        risk_lines
    )

    overall_emoji = risk_emoji(
        overall
    )

    # -----------------------------
    # LINKS
    # -----------------------------

    dex_url = (
        pair.get("url")
        or (
            "https://dexscreener.com/"
            f"solana/{mint}"
        )
    )

    rug_url = (
        f"https://rugcheck.xyz/tokens/"
        f"{mint}"
    )

    # -----------------------------
    # MESSAGE
    # -----------------------------

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

🛡️ RISK FLAGS

{risk_text}

{overall_emoji} OVERALL RISK: {overall}

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

    log(
        "🚀 SOLANA SCANNER STARTED"
    )

    log(
        f"Filters: MC "
        f"${MIN_MC:,} - "
        f"${MAX_MC:,} | "
        f"Liquidity >= "
        f"${MIN_LIQUIDITY:,}"
    )

    log(
        f"Previously alerted tokens: "
        f"{len(seen)}"
    )

    addresses = (
        get_latest_solana_tokens()
    )

    log(
        f"Discovered "
        f"{len(addresses)} "
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
            (pair.get("liquidity") or {})
            .get("usd")
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
            f"Liquidity="
            f"{format_money(liquidity)}"
        )

        # -----------------------------
        # SECURITY DATA
        # -----------------------------

        security = get_rugcheck_data(
            mint
        )

        # -----------------------------
        # DIRECT BLOCKCHAIN CHECK
        # -----------------------------

        authorities = (
            get_token_authorities(mint)
        )

        # -----------------------------
        # BUILD ALERT
        # -----------------------------

        message = build_alert(
            pair,
            mint,
            security,
            authorities
        )

        if send_telegram(message):

            log(
                f"Telegram alert sent: "
                f"{mint}"
            )

            seen.add(mint)

            alerts += 1

            save_seen(seen)

        else:

            log(
                f"Telegram failed: "
                f"{mint}"
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
            "❌ TELEGRAM_BOT_TOKEN "
            "is not set."
        )

        return

    if not TELEGRAM_CHAT_ID:

        log(
            "❌ TELEGRAM_CHAT_ID "
            "is not set."
        )

        return

    log(
        "Telegram credentials detected."
    )

    log(
        "Automatic scanner is ready."
    )

    log(
        f"Scanner will run every "
        f"{SCAN_INTERVAL // 60} minutes."
    )

    while True:

        try:

            scan()

        except Exception as e:

            log(
                f"Unexpected scanner "
                f"error: {e}"
            )

        log(
            f"Sleeping for "
            f"{SCAN_INTERVAL} seconds..."
        )

        time.sleep(
            SCAN_INTERVAL
        )


if __name__ == "__main__":
    main()
