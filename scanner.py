import os
import time
import json
import hashlib
import requests
from datetime import datetime, timezone


# ============================================================
# SETTINGS
# ============================================================

MIN_MC = 5_000
MAX_MC = 100_000
MIN_LIQUIDITY = 5_000

SCAN_INTERVAL = 300  # 5 minutes

REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 3

# How many recent Pump.fun transactions to inspect each scan.
PUMP_SIGNATURE_LIMIT = 100

# Only treat very recent Pump.fun launches as "new".
MAX_LAUNCH_AGE_MINUTES = 30


# ============================================================
# PUMP.FUN
# ============================================================

PUMP_FUN_PROGRAM = (
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
)

# Anchor instruction discriminators.
#
# create     = sha256("global:create")[:8]
# create_v2  = sha256("global:create_v2")[:8]

CREATE_DISCRIMINATOR = bytes([
    24, 30, 200, 40, 5, 28, 7, 119
])

CREATE_V2_DISCRIMINATOR = bytes([
    214, 144, 76, 236, 95, 139, 49, 180
])


# ============================================================
# RPC / APIs
# ============================================================

SOLANA_RPC = (
    "https://api.mainnet-beta.solana.com"
)

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
# LOCAL FILES
# ============================================================

SEEN_FILE = "seen_tokens.json"
PUMP_CURSOR_FILE = "pump_cursor.json"


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
# SESSION
# ============================================================

session = requests.Session()

session.headers.update({
    "User-Agent": "SolanaPumpScanner/5.0"
})


# ============================================================
# LOG
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
        return (
            f"${value / 1_000_000:.2f}M"
        )

    if value >= 1_000:
        return f"${value:,.0f}"

    return f"${value:.2f}"


def format_number(value):

    try:
        return f"{int(float(value)):,}"

    except Exception:
        return "Unknown"


# ============================================================
# NETWORK RETRY
# ============================================================

def request_get(url, **kwargs):

    timeout = kwargs.pop(
        "timeout",
        REQUEST_TIMEOUT
    )

    for attempt in range(
        1,
        MAX_RETRIES + 1
    ):

        try:

            response = session.get(
                url,
                timeout=timeout,
                **kwargs
            )

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            log(
                f"GET attempt "
                f"{attempt}/{MAX_RETRIES} failed: "
                f"{str(e)[:200]}"
            )

            if attempt < MAX_RETRIES:

                wait = (
                    RETRY_DELAY * attempt
                )

                log(
                    f"Retrying in {wait}s..."
                )

                time.sleep(wait)

    return None


def request_post(url, **kwargs):

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

            response.raise_for_status()

            return response

        except requests.RequestException as e:

            log(
                f"POST attempt "
                f"{attempt}/{MAX_RETRIES} failed: "
                f"{str(e)[:200]}"
            )

            if attempt < MAX_RETRIES:

                wait = (
                    RETRY_DELAY * attempt
                )

                log(
                    f"Retrying in {wait}s..."
                )

                time.sleep(wait)

    return None


# ============================================================
# BASE58 DECODER
# ============================================================

BASE58_ALPHABET = (
    "123456789ABCDEFGHJKLMNPQRSTUVWXYZ"
    "abcdefghijkmnopqrstuvwxyz"
)


def base58_decode(value):

    try:

        number = 0

        for character in value:

            number *= 58

            number += BASE58_ALPHABET.index(
                character
            )

        result = number.to_bytes(
            (number.bit_length() + 7) // 8,
            "big"
        )

        # Restore leading zero bytes.
        leading_zeroes = 0

        for character in value:

            if character == "1":
                leading_zeroes += 1
            else:
                break

        return (
            b"\x00" * leading_zeroes
            + result
        )

    except Exception:

        return b""


# ============================================================
# BASE58 ENCODE
# ============================================================

def base58_encode(data):

    if not data:
        return ""

    number = int.from_bytes(
        data,
        "big"
    )

    result = ""

    while number > 0:

        number, remainder = divmod(
            number,
            58
        )

        result = (
            BASE58_ALPHABET[remainder]
            + result
        )

    leading_zeroes = 0

    for byte in data:

        if byte == 0:
            leading_zeroes += 1
        else:
            break

    return (
        "1" * leading_zeroes
        + result
    )


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
# PUMP CURSOR
# ============================================================

def load_pump_cursor():

    try:

        if not os.path.exists(
            PUMP_CURSOR_FILE
        ):
            return None

        with open(
            PUMP_CURSOR_FILE,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

        return data.get(
            "signature"
        )

    except Exception:

        return None


def save_pump_cursor(signature):

    try:

        with open(
            PUMP_CURSOR_FILE,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                {
                    "signature": signature
                },
                file
            )

    except Exception as e:

        log(
            f"Could not save Pump cursor: {e}"
        )


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

        return data.get(
            "result"
        )

    except Exception as e:

        log(
            f"RPC JSON error: {e}"
        )

        return None


# ============================================================
# GET PUMP TRANSACTION SIGNATURES
# ============================================================

def get_pump_signatures():

    cursor = load_pump_cursor()

    config = {
        "limit": PUMP_SIGNATURE_LIMIT,
        "commitment": "confirmed"
    }

    if cursor:

        config["until"] = cursor

    result = rpc_call(
        "getSignaturesForAddress",
        [
            PUMP_FUN_PROGRAM,
            config
        ]
    )

    if not result:

        return []

    return result


# ============================================================
# GET TRANSACTION
# ============================================================

def get_transaction(signature):

    return rpc_call(
        "getTransaction",
        [
            signature,
            {
                "encoding": "json",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0
            }
        ]
    )


# ============================================================
# ACCOUNT KEY EXTRACTION
# ============================================================

def get_all_account_keys(tx):

    transaction = (
        tx.get("transaction")
        or {}
    )

    message = (
        transaction.get("message")
        or {}
    )

    keys = []

    # Normal transaction keys.
    account_keys = (
        message.get("accountKeys")
        or []
    )

    for key in account_keys:

        if isinstance(
            key,
            str
        ):

            keys.append(key)

        elif isinstance(
            key,
            dict
        ):

            pubkey = key.get(
                "pubkey"
            )

            if pubkey:
                keys.append(pubkey)

    # Versioned transaction loaded addresses.
    meta = tx.get("meta") or {}

    loaded = (
        meta.get("loadedAddresses")
        or {}
    )

    writable = (
        loaded.get("writable")
        or []
    )

    readonly = (
        loaded.get("readonly")
        or []
    )

    keys.extend(writable)
    keys.extend(readonly)

    return keys


# ============================================================
# GET ALL INSTRUCTIONS
# ============================================================

def get_all_instructions(tx):

    transaction = (
        tx.get("transaction")
        or {}
    )

    message = (
        transaction.get("message")
        or {}
    )

    instructions = []

    # Top-level instructions.
    instructions.extend(
        message.get(
            "instructions"
        )
        or []
    )

    # Inner instructions.
    meta = tx.get("meta") or {}

    inner_groups = (
        meta.get("innerInstructions")
        or []
    )

    for group in inner_groups:

        instructions.extend(
            group.get(
                "instructions"
            )
            or []
        )

    return instructions


# ============================================================
# FIND PUMP CREATE INSTRUCTION
# ============================================================

def extract_pump_mint(tx):

    if not tx:

        return None

    meta = tx.get("meta") or {}

    if meta.get("err") is not None:

        return None

    account_keys = (
        get_all_account_keys(tx)
    )

    if not account_keys:

        return None

    instructions = (
        get_all_instructions(tx)
    )

    for instruction in instructions:

        if not isinstance(
            instruction,
            dict
        ):
            continue

        # JSON encoding gives us:
        #
        # programIdIndex
        # accounts
        # data

        program_index = instruction.get(
            "programIdIndex"
        )

        accounts = (
            instruction.get(
                "accounts"
            )
            or []
        )

        data = instruction.get(
            "data"
        )

        if program_index is None:
            continue

        if program_index >= len(
            account_keys
        ):
            continue

        program_id = account_keys[
            program_index
        ]

        if program_id != PUMP_FUN_PROGRAM:
            continue

        if not isinstance(
            data,
            str
        ):
            continue

        raw_data = base58_decode(
            data
        )

        if len(raw_data) < 8:
            continue

        discriminator = raw_data[:8]

        is_create = (
            discriminator
            == CREATE_DISCRIMINATOR
        )

        is_create_v2 = (
            discriminator
            == CREATE_V2_DISCRIMINATOR
        )

        if not (
            is_create
            or is_create_v2
        ):
            continue

        # Pump.fun IDL:
        #
        # create:
        # account #1 = mint
        #
        # create_v2:
        # account #1 = mint

        if not accounts:
            continue

        mint_index = accounts[0]

        if mint_index >= len(
            account_keys
        ):
            continue

        mint = account_keys[
            mint_index
        ]

        if not mint:
            continue

        if is_create:

            log(
                f"🎯 Pump.fun CREATE detected: "
                f"{mint}"
            )

        else:

            log(
                f"🎯 Pump.fun CREATE_V2 detected: "
                f"{mint}"
            )

        return mint

    return None


# ============================================================
# DISCOVER NEW PUMP.FUN TOKENS
# ============================================================

def discover_new_pump_tokens():

    signatures = (
        get_pump_signatures()
    )

    if not signatures:

        log(
            "No Pump.fun signatures returned."
        )

        return []

    tokens = []

    newest_signature = (
        signatures[0].get(
            "signature"
        )
    )

    # RPC returns newest -> oldest.
    #
    # Process oldest -> newest so alerts
    # appear in launch order.

    ordered = list(
        reversed(signatures)
    )

    now = datetime.now(
        timezone.utc
    ).timestamp()

    for item in ordered:

        signature = item.get(
            "signature"
        )

        if not signature:
            continue

        # Ignore failed transactions.
        if item.get("err") is not None:
            continue

        block_time = item.get(
            "blockTime"
        )

        # Only consider recent launches.
        if block_time:

            age_minutes = (
                now - block_time
            ) / 60

            if (
                age_minutes
                > MAX_LAUNCH_AGE_MINUTES
            ):

                continue

        tx = get_transaction(
            signature
        )

        if not tx:

            continue

        mint = extract_pump_mint(
            tx
        )

        if not mint:

            continue

        tokens.append({
            "mint": mint,
            "signature": signature,
            "block_time": block_time
        })

    # The oldest signature is the cursor
    # boundary for the next scan.
    #
    # We save the OLDEST signature we
    # processed, not the newest.

    oldest_signature = (
        signatures[-1].get(
            "signature"
        )
    )

    if oldest_signature:

        save_pump_cursor(
            oldest_signature
        )

    unique = {}

    for token in tokens:

        unique[
            token["mint"]
        ] = token

    return list(
        unique.values()
    )


# ============================================================
# DEXSCREENER
# ============================================================

def get_token_pair(mint):

    response = request_get(
        DEXSCREENER_TOKEN + mint
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
            if pair.get("chainId")
            == "solana"
        ]

        if not solana_pairs:

            log(
                f"⏳ No DexScreener pair yet: "
                f"{mint}"
            )

            return None

        solana_pairs.sort(
            key=lambda p:
            safe_float(
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
        RUGCHECK_REPORT.format(mint)
    )

    if not response:

        return result

    try:

        data = response.json()

        # HOLDERS
        holder_count = (
            data.get("totalHolders")
            or data.get("holderCount")
            or data.get("holdersCount")
        )

        if holder_count is not None:

            result["holders"] = (
                holder_count
            )

        # TOP 10
        top_holders = data.get(
            "topHolders"
        )

        if (
            isinstance(
                top_holders,
                list
            )
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
                    or holder.get(
                        "percentage"
                    )
                    or holder.get(
                        "ownershipPercentage"
                    )
                )

                # Convert 0.25 -> 25%.
                if (
                    0 < percentage <= 1
                ):

                    percentage *= 100

                total_percentage += (
                    percentage
                )

            if total_percentage > 0:

                result["top10"] = (
                    total_percentage
                )

        # RISK DATA
        risks = data.get(
            "risks"
        )

        if isinstance(
            risks,
            list
        ):

            names = []

            for risk in risks:

                if not isinstance(
                    risk,
                    dict
                ):
                    continue

                name = (
                    risk.get("name")
                    or risk.get(
                        "description"
                    )
                    or risk.get("level")
                )

                if name:

                    names.append(
                        str(name)
                    )

            if names:

                result["risk"] = (
                    ", ".join(
                        names[:5]
                    )
                )

        return result

    except Exception as e:

        log(
            f"RugCheck JSON error: {e}"
        )

        return result


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

        return (
            f"{age_minutes} min"
        )

    hours = age_minutes // 60

    if hours < 24:

        return (
            f"{hours} hr"
        )

    days = hours // 24

    return (
        f"{days} day"
    )


# ============================================================
# RISK FLAGS
# ============================================================

def evaluate_top10(top10):

    if top10 is None:

        return "⚪ Top 10: Unknown"

    if top10 <= TOP10_GREEN:

        return (
            f"🟢 Top 10: "
            f"{top10:.1f}%"
        )

    if top10 <= TOP10_YELLOW:

        return (
            f"🟡 Top 10: "
            f"{top10:.1f}%"
        )

    return (
        f"🔴 Top 10: "
        f"{top10:.1f}%"
    )


def evaluate_liquidity_mc(
    liquidity,
    mc
):

    if mc <= 0:

        return (
            "⚪ Liquidity/MC: Unknown"
        )

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

        return (
            "⚪ Volume/MC: Unknown"
        )

    ratio = (
        volume / mc
    )

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

        return (
            "⚪ Holders: Unknown"
        )

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

        return (
            "⚪ Age: Unknown"
        )

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


def calculate_overall_risk(
    levels
):

    high = levels.count(
        "HIGH"
    )

    medium = levels.count(
        "MEDIUM"
    )

    if high >= 2:

        return "HIGH"

    if high >= 1 and medium >= 1:

        return "HIGH"

    if high == 1:

        return "MEDIUM"

    if medium >= 2:

        return "MEDIUM"

    if medium == 1:

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

        levels.append(
            top10_level
        )

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

        levels.append(
            liq_level
        )

    # VOLUME / MC
    if mc <= 0:

        vol_level = "UNKNOWN"

    else:

        ratio = (
            volume / mc
        )

        if ratio <= VOL_MC_GREEN_MAX:

            vol_level = "LOW"

        elif ratio <= VOL_MC_YELLOW_MAX:

            vol_level = "MEDIUM"

        else:

            vol_level = "HIGH"

    if vol_level != "UNKNOWN":

        levels.append(
            vol_level
        )

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

        levels.append(
            holder_level
        )

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

        levels.append(
            age_level
        )

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
            "ERROR: TELEGRAM_BOT_TOKEN missing"
        )

        return False

    if not TELEGRAM_CHAT_ID:

        log(
            "ERROR: TELEGRAM_CHAT_ID missing"
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
            f"Telegram API error: "
            f"{data}"
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

    if holders is None:

        holders_text = "Unknown"

    else:

        holders_text = format_number(
            holders
        )

    top10 = security.get(
        "top10"
    )

    if top10 is None:

        top10_text = "Unknown"

    else:

        top10_text = (
            f"{top10:.1f}%"
        )

    if authorities.get(
        "success"
    ):

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

    risk_text, overall = (
        build_risk_flags(
            top10,
            liquidity,
            mc,
            volume,
            holders,
            age_minutes
        )
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
👥 Holders: {holders_text}
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
# PROCESS TOKEN
# ============================================================

def process_token(
    mint,
    seen
):

    if mint in seen:

        return False

    # Give DexScreener time to index
    # a brand-new Pump.fun token.

    pair = None

    for attempt in range(
        1,
        4
    ):

        pair = get_token_pair(
            mint
        )

        if pair:

            break

        log(
            f"Waiting for DexScreener "
            f"indexing "
            f"({attempt}/3): "
            f"{mint}"
        )

        time.sleep(5)

    if not pair:

        log(
            f"Skipped — no DexScreener "
            f"pair yet: {mint}"
        )

        return False

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

    # MARKET CAP
    if mc < MIN_MC:

        log(
            f"Filtered MC below "
            f"${MIN_MC:,}: "
            f"{mint} | "
            f"{format_money(mc)}"
        )

        return False

    if mc > MAX_MC:

        log(
            f"Filtered MC above "
            f"${MAX_MC:,}: "
            f"{mint} | "
            f"{format_money(mc)}"
        )

        return False

    # LIQUIDITY
    if liquidity < MIN_LIQUIDITY:

        log(
            f"Filtered liquidity: "
            f"{mint} | "
            f"{format_money(liquidity)}"
        )

        return False

    log(
        f"🔥 QUALIFIED PUMP.FUN TOKEN: "
        f"{mint} | "
        f"MC={format_money(mc)} | "
        f"Liquidity="
        f"{format_money(liquidity)}"
    )

    # SECURITY
    security = get_rugcheck_data(
        mint
    )

    authorities = (
        get_token_authorities(
            mint
        )
    )

    message = build_alert(
        pair,
        mint,
        security,
        authorities
    )

    # TELEGRAM
    if send_telegram(message):

        log(
            f"✅ Telegram alert sent: "
            f"{mint}"
        )

        # Only mark as seen after
        # successful Telegram delivery.

        seen.add(mint)

        save_seen(seen)

        return True

    log(
        f"❌ Telegram failed: "
        f"{mint}"
    )

    return False


# ============================================================
# SCAN
# ============================================================

def scan():

    seen = load_seen()

    log("=" * 70)

    log(
        "🚀 PUMP.FUN DIRECT SCANNER"
    )

    log(
        "Detection: ON-CHAIN "
        "CREATE + CREATE_V2"
    )

    log(
        f"Market Cap: "
        f"${MIN_MC:,} - "
        f"${MAX_MC:,}"
    )

    log(
        f"Minimum Liquidity: "
        f"${MIN_LIQUIDITY:,}"
    )

    log(
        f"Previously alerted: "
        f"{len(seen)}"
    )

    tokens = (
        discover_new_pump_tokens()
    )

    log(
        f"New Pump.fun launches detected: "
        f"{len(tokens)}"
    )

    checked = 0
    alerts = 0

    for token in tokens:

        mint = token.get(
            "mint"
        )

        if not mint:

            continue

        checked += 1

        try:

            if process_token(
                mint,
                seen
            ):

                alerts += 1

        except Exception as e:

            log(
                f"Processing error "
                f"for {mint}: {e}"
            )

    save_seen(seen)

    log("=" * 70)

    log(
        f"SCAN COMPLETE | "
        f"Pump launches checked: "
        f"{checked} | "
        f"New alerts: {alerts} | "
        f"Seen: {len(seen)}"
    )

    log("=" * 70)


# ============================================================
# MAIN
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
        "✅ Direct Pump.fun scanner ready."
    )

    log(
        "Works with GitHub Actions "
        "and Termux."
    )

    log(
        f"Scanning every "
        f"{SCAN_INTERVAL // 60} minutes."
    )

    while True:

        try:

            scan()

        except Exception as e:

            log(
                f"Unexpected scanner error: "
                f"{e}"
            )

        log(
            f"Sleeping for "
            f"{SCAN_INTERVAL} seconds..."
        )

        time.sleep(
            SCAN_INTERVAL
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
