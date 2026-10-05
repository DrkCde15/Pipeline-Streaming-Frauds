-- Schema PostgreSQL unificado (negócio + ML real ULB)
-- Uso: psql $DATABASE_URL -f db/schema.sql

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY,
    user_id        INTEGER NOT NULL,
    valor          NUMERIC(12,2) NOT NULL,
    moeda          CHAR(3) NOT NULL DEFAULT 'BRL',
    timestamp      TIMESTAMPTZ NOT NULL,
    cidade         TEXT NOT NULL DEFAULT 'Desconhecida',
    pais           CHAR(2) NOT NULL DEFAULT 'BR',
    latitude       DOUBLE PRECISION NOT NULL DEFAULT 0,
    longitude      DOUBLE PRECISION NOT NULL DEFAULT 0,
    dispositivo    TEXT NOT NULL DEFAULT 'unknown',
    categoria      TEXT NOT NULL DEFAULT 'outros',
    is_fraud       BOOLEAN NOT NULL DEFAULT FALSE,
    source         TEXT NOT NULL DEFAULT 'synthetic'
        CHECK (source IN ('synthetic', 'creditcard_csv')),
    time_original  DOUBLE PRECISION,          -- Time do CSV (segundos)
    -- Features PCA anonimizadas (NULL quando sintética)
    V1  DOUBLE PRECISION,  V2  DOUBLE PRECISION,
    V3  DOUBLE PRECISION,  V4  DOUBLE PRECISION,
    V5  DOUBLE PRECISION,  V6  DOUBLE PRECISION,
    V7  DOUBLE PRECISION,  V8  DOUBLE PRECISION,
    V9  DOUBLE PRECISION,  V10 DOUBLE PRECISION,
    V11 DOUBLE PRECISION,  V12 DOUBLE PRECISION,
    V13 DOUBLE PRECISION,  V14 DOUBLE PRECISION,
    V15 DOUBLE PRECISION,  V16 DOUBLE PRECISION,
    V17 DOUBLE PRECISION,  V18 DOUBLE PRECISION,
    V19 DOUBLE PRECISION,  V20 DOUBLE PRECISION,
    V21 DOUBLE PRECISION,  V22 DOUBLE PRECISION,
    V23 DOUBLE PRECISION,  V24 DOUBLE PRECISION,
    V25 DOUBLE PRECISION,  V26 DOUBLE PRECISION,
    V27 DOUBLE PRECISION,  V28 DOUBLE PRECISION,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_user_ts
    ON transactions (user_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_ts
    ON transactions (timestamp);
CREATE INDEX IF NOT EXISTS idx_transactions_fraud
    ON transactions (is_fraud) WHERE is_fraud;

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id UUID NOT NULL REFERENCES transactions(transaction_id),
    regras_ativadas TEXT[] NOT NULL DEFAULT '{}',
    ml_probabilidade DOUBLE PRECISION,
    score          DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_tx
    ON fraud_alerts (transaction_id);
