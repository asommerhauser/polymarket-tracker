CREATE SCHEMA IF NOT EXISTS pm;

-- USERS
CREATE TABLE IF NOT EXISTS pm.users (
  user_hash TEXT PRIMARY KEY,          -- your stable hash id
  display_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- WALLETS
CREATE TABLE IF NOT EXISTS pm.wallets (
  wallet_id TEXT PRIMARY KEY,          -- the wallet string
  user_hash TEXT NOT NULL REFERENCES pm.users(user_hash) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fast lookups in either direction
CREATE INDEX IF NOT EXISTS wallets_user_idx ON pm.wallets(user_hash);