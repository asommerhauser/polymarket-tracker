CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE SCHEMA IF NOT EXISTS pm;

-- =========================
-- USERS
-- =========================
CREATE TABLE IF NOT EXISTS pm.users (
  user_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  display_name TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================
-- WALLETS
-- =========================
CREATE TABLE IF NOT EXISTS pm.wallets (
  wallet_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  wallet_address TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =========================
-- BETS
-- =========================
CREATE TABLE IF NOT EXISTS pm.bets (
  bet_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

  user_id UUID NOT NULL,
  wallet_id UUID NOT NULL,

  bet_timestamp TIMESTAMPTZ NOT NULL,
  cost NUMERIC(20, 8) NOT NULL CHECK (cost >= 0),

  title TEXT,
  outcome TEXT,
  side TEXT,
  asset TEXT,
  condition_id TEXT,

  CONSTRAINT bets_user_fk
    FOREIGN KEY (user_id)
    REFERENCES pm.users(user_id)
    ON DELETE CASCADE,

  CONSTRAINT bets_wallet_fk
    FOREIGN KEY (wallet_id)
    REFERENCES pm.wallets(wallet_id)
    ON DELETE RESTRICT
);