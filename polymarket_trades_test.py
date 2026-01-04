import requests
import pandas as pd

BASE_URL = "https://data-api.polymarket.com/trades"

PARAMS = {
    "limit": 9999,
    "offset": 0
}

DROP_COLUMNS = [
    "slug",
    "icon",
    "outcomeIndex",
    "pseudonym",
    "bio",
    "profileImage",
    "profileImageOptimized",
    "transactionHash",
]

def main():
    resp = requests.get(BASE_URL, params=PARAMS, timeout=30)
    resp.raise_for_status()

    trades = resp.json()

    print(f"Pulled {len(trades)} trades\n")

    # Normalize into DataFrame
    df = pd.json_normalize(trades)

    # Drop unwanted columns
    df = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns])

    # Ensure numeric types
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["size"] = pd.to_numeric(df["size"], errors="coerce")

    # Compute cost
    df["cost"] = df["price"] * df["size"]

    # Convert timestamp (ms → datetime)
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    # Sort by cost
    df = df.sort_values(by="cost", ascending=False).reset_index(drop=True)

    print("Top 5 trades by cost:")
    print(df[["timestamp", "datetime", "price", "size", "cost"]].head())

    # Save to CSV
    df.to_csv("polymarket_trades_sample.csv", index=False)
    print("\nSaved to polymarket_trades_sample.csv")


if __name__ == "__main__":
    main()