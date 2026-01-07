# Polymarket Expensive Bets Tracker (PostgreSQL + Python)

This project pulls recent trades from Polymarket’s public trades API and stores only **expensive bets** (by default, `cost >= 750`) in a local **PostgreSQL** database.

Right now it’s intentionally simple + manual:
- Fetch latest trades from the API (`/trades`)
- Normalize them into a dataframe
- Compute `cost = price * size`
- **Upsert all users + wallets** seen in the pull
- **Insert only qualifying bets** into `pm.bets`
- **Only create events** if they have at least one qualifying bet
- Dedupe bets by `transaction_hash`

Future work could include automation (scheduled ingestion, websockets/streaming, dashboards, alerts, etc.).

---

## What gets stored?

### Always stored
- `pm.users` (unique by `display_name`)
- `pm.wallets` (unique by `wallet_address`)

### Stored only when bet is expensive (`cost >= 750`)
- `pm.events` (unique by `event_slug`)
- `pm.bets` (deduped by `transaction_hash`)

---

## Requirements

- Python 3.10+ recommended
- PostgreSQL 13+ recommended
- A database user/password you can connect with

---

## Install dependencies

Create and activate a virtual environment (optional but recommended), then install requirements:

```bash
pip install -r requirements.txt
```

---

## Database Setup

1. Create Postgres database
2. Run the schema SQL for set up.

---

## Enviornment Variables (.env)

```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=polymarket_tracker
DB_USER=postgres
DB_PASSWORD=your_password_here
```