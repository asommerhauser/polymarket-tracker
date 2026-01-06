import os
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data-api.polymarket.com/trades"
PARAMS = {"limit": 9999, "offset": 0}

COST_THRESHOLD = 750.0
LOCAL_TZ = "America/Los_Angeles"

# Don't drop transactionHash anymore (we use it for dedupe)
DROP_COLUMNS = [
    "slug",
    "icon",
    "outcomeIndex",
    "pseudonym",
    "bio",
    "profileImage",
    "profileImageOptimized",
]

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

def normalize_trades_to_df(trades: list[dict]) -> pd.DataFrame:
    df = pd.json_normalize(trades)

    # Drop unwanted columns if present
    df = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns], errors="ignore")

    # Ensure required columns exist (API might vary; be defensive)
    required = ["name", "proxyWallet", "eventSlug", "price", "size", "timestamp"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required fields from API response: {missing}")

    # Numerics
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["size"] = pd.to_numeric(df["size"], errors="coerce")
    df["cost"] = df["price"] * df["size"]

    # Timestamp -> localized datetime
    df["bet_timestamp"] = (
        pd.to_datetime(df["timestamp"], unit="s", utc=True)
        .dt.tz_convert(LOCAL_TZ)
    )

    # Keep only rows that have usable numeric values
    df = df.dropna(subset=["price", "size", "cost", "bet_timestamp"])

    return df

def upsert_users(cur, display_names: list[str]):
    # Insert users (do nothing on conflict)
    execute_batch(
        cur,
        """
        INSERT INTO pm.users (display_name)
        VALUES (%s)
        ON CONFLICT (display_name) DO NOTHING
        """,
        [(n,) for n in display_names],
        page_size=1000,
    )

def upsert_wallets(cur, wallet_addresses: list[str]):
    execute_batch(
        cur,
        """
        INSERT INTO pm.wallets (wallet_address)
        VALUES (%s)
        ON CONFLICT (wallet_address) DO NOTHING
        """,
        [(w,) for w in wallet_addresses],
        page_size=1000,
    )

def upsert_events(cur, event_slugs: list[str]):
    execute_batch(
        cur,
        """
        INSERT INTO pm.events (event_slug)
        VALUES (%s)
        ON CONFLICT (event_slug) DO NOTHING
        """,
        [(s,) for s in event_slugs],
        page_size=1000,
    )

def fetch_id_map(cur, table: str, natural_col: str, id_col: str, naturals: list[str]) -> dict[str, str]:
    # Returns { natural_value: uuid }
    if not naturals:
        return {}

    cur.execute(
        f"""
        SELECT {natural_col}, {id_col}
        FROM {table}
        WHERE {natural_col} = ANY(%s)
        """,
        (naturals,),
    )
    return {row[0]: row[1] for row in cur.fetchall()}

def insert_qualifying_bets(cur, rows: list[tuple]):
    """
    rows:
      (user_id, wallet_id, event_id, bet_timestamp, cost, transaction_hash,
       title, outcome, side, asset, condition_id, price, size)
    """
    execute_batch(
        cur,
        """
        INSERT INTO pm.bets (
          user_id, wallet_id, event_id,
          bet_timestamp, cost,
          transaction_hash,
          title, outcome, side, asset, condition_id,
          price, size
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (transaction_hash) DO NOTHING
        """,
        rows,
        page_size=1000,
    )

def main():
    # Pull trades
    resp = requests.get(BASE_URL, params=PARAMS, timeout=30)
    resp.raise_for_status()
    trades = resp.json()
    print(f"Pulled {len(trades)} trades")

    # Normalize
    df = normalize_trades_to_df(trades)

    # ALWAYS store users + wallets (even if they have no qualifying bets)
    all_users = sorted(df["name"].dropna().astype(str).unique().tolist())
    all_wallets = sorted(df["proxyWallet"].dropna().astype(str).unique().tolist())

    # Filter qualifying bets
    qualifying = df[df["cost"] >= COST_THRESHOLD].copy()

    # Events should only exist if they have >= 1 qualifying bet
    qualifying_event_slugs = sorted(qualifying["eventSlug"].dropna().astype(str).unique().tolist())

    print(f"Total users found: {len(all_users)} (will upsert all)")
    print(f"Total wallets found: {len(all_wallets)} (will upsert all)")
    print(f"Qualifying bets (cost >= {COST_THRESHOLD}): {len(qualifying)}")
    print(f"Qualifying events: {len(qualifying_event_slugs)}")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Upsert users + wallets always
            upsert_users(cur, all_users)
            upsert_wallets(cur, all_wallets)

            # Upsert events ONLY for qualifying bets
            if qualifying_event_slugs:
                upsert_events(cur, qualifying_event_slugs)

            # Build UUID maps (natural key -> uuid)
            user_map = fetch_id_map(cur, "pm.users", "display_name", "user_id", all_users)
            wallet_map = fetch_id_map(cur, "pm.wallets", "wallet_address", "wallet_id", all_wallets)
            event_map = fetch_id_map(cur, "pm.events", "event_slug", "event_id", qualifying_event_slugs)

            # Prepare bet insert rows ONLY for qualifying bets
            bet_rows = []
            for _, r in qualifying.iterrows():
                name = str(r["name"])
                wallet = str(r["proxyWallet"])
                slug = str(r["eventSlug"])

                user_id = user_map.get(name)
                wallet_id = wallet_map.get(wallet)
                event_id = event_map.get(slug)

                # If event_id is missing, skip (shouldn't happen if upsert succeeded)
                if not (user_id and wallet_id and event_id):
                    continue

                bet_rows.append((
                    user_id,
                    wallet_id,
                    event_id,
                    r["bet_timestamp"].to_pydatetime(),
                    float(r["cost"]),
                    # API field name is usually "transactionHash"
                    r.get("transactionHash", None) if isinstance(r, dict) else (r["transactionHash"] if "transactionHash" in qualifying.columns else None),
                    r.get("title", None),
                    r.get("outcome", None),
                    r.get("side", None),
                    r.get("asset", None),
                    r.get("conditionId", None),
                    float(r["price"]) if pd.notna(r["price"]) else None,
                    float(r["size"]) if pd.notna(r["size"]) else None,
                ))

            # Insert bets
            if bet_rows:
                insert_qualifying_bets(cur, bet_rows)

        conn.commit()

    print("Ingestion complete.")

if __name__ == "__main__":
    main()