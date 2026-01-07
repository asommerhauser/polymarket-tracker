import os
import time
import random
import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data-api.polymarket.com/trades"

COST_THRESHOLD = 250.0
LOCAL_TZ = "America/Los_Angeles"

# Defensive: keep these modest; /trades supports up to 10k, but huge pages can be slow + memory-heavy.
PAGE_LIMIT = int(os.getenv("TRADES_PAGE_LIMIT", "2000"))

# Rate limiting: Polymarket docs say /trades 200 requests / 10s. We'll stay way under that.
# e.g. 0.10s => 100 req/10s (plus jitter).
SLEEP_SECONDS = float(os.getenv("TRADES_SLEEP_SECONDS", "0.12"))
MAX_PAGES = int(os.getenv("TRADES_MAX_PAGES", "5000"))  # safety valve

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

def get_latest_bet_timestamp_utc(cur) -> pd.Timestamp:
    """
    Returns latest bet_timestamp in UTC as pandas Timestamp (tz-aware).
    If no rows, returns epoch start.
    """
    cur.execute("SELECT MAX(bet_timestamp) FROM pm.bets;")
    row = cur.fetchone()
    if not row or row[0] is None:
        return pd.Timestamp(0, unit="s", tz="UTC")
    # row[0] is a python datetime (tz-aware if stored as timestamptz)
    ts = pd.Timestamp(row[0])
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")

def fetch_trades_page(offset: int, limit: int, session: requests.Session) -> list[dict]:
    params = {"limit": limit, "offset": offset}
    resp = session.get(BASE_URL, params=params, timeout=30)

    # If Cloudflare throttles you, you may see 429/403/5xx depending on behavior.
    # Handle gently with backoff.
    if resp.status_code in (429, 500, 502, 503, 504):
        raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)

    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("Unexpected response shape; expected a list of trades.")
    return data

def normalize_trades_to_df(trades: list[dict]) -> pd.DataFrame:
    if not trades:
        return pd.DataFrame()

    df = pd.json_normalize(trades)
    df = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns], errors="ignore")

    required = ["name", "proxyWallet", "eventSlug", "price", "size", "timestamp"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required fields from API response: {missing}")

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["size"] = pd.to_numeric(df["size"], errors="coerce")
    df["cost"] = df["price"] * df["size"]

    # Create both UTC (for stopping logic) and local (for storage/use)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["bet_timestamp"] = df["timestamp_utc"].dt.tz_convert(LOCAL_TZ)

    df = df.dropna(subset=["price", "size", "cost", "bet_timestamp", "timestamp_utc"])
    return df

def upsert_users(cur, display_names: list[str]):
    if not display_names:
        return
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
    if not wallet_addresses:
        return
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
    if not event_slugs:
        return
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
    if not rows:
        return
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
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            latest_db_ts_utc = get_latest_bet_timestamp_utc(cur)

    print(f"Latest bet_timestamp in DB (UTC): {latest_db_ts_utc}")

    session = requests.Session()

    offset = 0
    pages = 0
    total_trades_seen = 0
    total_qualifying_insert_rows = 0

    # Backoff state
    backoff = 0.5
    backoff_max = 10.0

    while pages < MAX_PAGES:
        pages += 1

        # Rate-limit (jitter helps avoid “thundering herd” patterns)
        time.sleep(SLEEP_SECONDS + random.uniform(0, SLEEP_SECONDS * 0.25))

        try:
            trades = fetch_trades_page(offset=offset, limit=PAGE_LIMIT, session=session)
            backoff = 0.5  # reset after a good request
        except requests.HTTPError as e:
            # Respect throttling by backing off
            print(f"[WARN] Request failed at offset={offset}. {e}. Backing off {backoff:.2f}s")
            time.sleep(backoff + random.uniform(0, 0.25))
            backoff = min(backoff * 2, backoff_max)
            continue

        if not trades:
            print("No more trades returned; stopping.")
            break

        total_trades_seen += len(trades)

        df = normalize_trades_to_df(trades)
        if df.empty:
            print(f"Empty normalized page at offset={offset}; stopping.")
            break

        # API returns newest-first (assumption). Determine oldest timestamp in this page (UTC).
        oldest_page_ts_utc = df["timestamp_utc"].min()
        newest_page_ts_utc = df["timestamp_utc"].max()

        # Keep only trades newer than what we already have
        df_new = df[df["timestamp_utc"] > latest_db_ts_utc].copy()

        # Upsert users + wallets always from df_new? (or all df)
        # You said: ALWAYS store users + wallets (even if no qualifying bets).
        # That only matters for "new trades"; doing it for all fetched pages is also fine but grows work.
        all_users = sorted(df_new["name"].dropna().astype(str).unique().tolist())
        all_wallets = sorted(df_new["proxyWallet"].dropna().astype(str).unique().tolist())

        qualifying = df_new[df_new["cost"] >= COST_THRESHOLD].copy()
        qualifying_event_slugs = sorted(qualifying["eventSlug"].dropna().astype(str).unique().tolist())

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                upsert_users(cur, all_users)
                upsert_wallets(cur, all_wallets)
                upsert_events(cur, qualifying_event_slugs)

                user_map = fetch_id_map(cur, "pm.users", "display_name", "user_id", all_users)
                wallet_map = fetch_id_map(cur, "pm.wallets", "wallet_address", "wallet_id", all_wallets)
                event_map = fetch_id_map(cur, "pm.events", "event_slug", "event_id", qualifying_event_slugs)

                bet_rows = []
                for _, r in qualifying.iterrows():
                    name = str(r["name"])
                    wallet = str(r["proxyWallet"])
                    slug = str(r["eventSlug"])

                    user_id = user_map.get(name)
                    wallet_id = wallet_map.get(wallet)
                    event_id = event_map.get(slug)

                    if not (user_id and wallet_id and event_id):
                        continue

                    bet_rows.append((
                        user_id,
                        wallet_id,
                        event_id,
                        r["bet_timestamp"].to_pydatetime(),
                        float(r["cost"]),
                        r.get("transactionHash", None),
                        r.get("title", None),
                        r.get("outcome", None),
                        r.get("side", None),
                        r.get("asset", None),
                        r.get("conditionId", None),
                        float(r["price"]) if pd.notna(r["price"]) else None,
                        float(r["size"]) if pd.notna(r["size"]) else None,
                    ))

                insert_qualifying_bets(cur, bet_rows)
                conn.commit()

        total_qualifying_insert_rows += len(qualifying)

        print(
            f"Page {pages} offset={offset} pulled={len(trades)} "
            f"utc_range=[{oldest_page_ts_utc} .. {newest_page_ts_utc}] "
            f"new_trades={len(df_new)} qualifying_new={len(qualifying)}"
        )

        # Stop condition:
        # If this page already includes timestamps at/older than latest_db_ts_utc,
        # then the *next* pages will be even older (assuming newest-first), so we’re done.
        if oldest_page_ts_utc <= latest_db_ts_utc:
            print("Reached already-ingested time window; stopping.")
            break

        offset += PAGE_LIMIT

        # Hard constraint: docs say offset <= 10000. Stop before we exceed it.
        # (/trades doc shows offset required range 0..10000)
        if offset > 10000:
            print( 
                "Hit Data API offset limit (offset > 10000). "
                "To go deeper historically you’ll need a different strategy (e.g., market-by-market pulls or CLOB/RTDS)."
            )
            break

    print(f"Done. Total trades seen={total_trades_seen}, total qualifying rows processed={total_qualifying_insert_rows}")

if __name__ == "__main__":
    main()