"""Writer Postgres idempotente para o schema unificado.

Tabelas criadas por db/schema.sql (auto-aplicado no compose).
INSERTs com ON CONFLICT DO NOTHING para replay seguro.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_TX_COLS = [
    "transaction_id",
    "user_id",
    "valor",
    "timestamp",
    "cidade",
    "pais",
    "latitude",
    "longitude",
    "dispositivo",
    "categoria",
    "is_fraud",
    "source",
    "time_original",
] + [f"V{i}" for i in range(1, 29)]


def get_conn():
    """Abre conexão com o DSN do config (import tardio p/ não quebrar sem psycopg2)."""
    import psycopg2

    from config.db_config import POSTGRES_CONFIG

    return psycopg2.connect(POSTGRES_CONFIG.dsn)


def _row_from_tx(tx: dict[str, Any]) -> tuple:
    loc = tx.get("localizacao", {}) or {}
    row = [
        tx["transaction_id"],
        int(tx["user_id"]),
        float(tx["valor"]),
        tx["timestamp"],
        loc.get("cidade", "Desconhecida"),
        loc.get("pais", "BR"),
        float(loc.get("latitude", 0.0)),
        float(loc.get("longitude", 0.0)),
        tx.get("dispositivo", "unknown"),
        tx.get("categoria", "outros"),
        bool(tx.get("is_fraud", False)),
        tx.get("source", "synthetic"),
        tx.get("time_original"),
    ]
    row += [tx.get(f"V{i}") for i in range(1, 29)]
    return tuple(row)


def salvar_transacao(conn, tx: dict[str, Any]) -> bool:
    """Insere transação. Retorna True se inseriu, False se já existia."""
    cols = ", ".join(_TX_COLS)
    placeholders = ", ".join(["%s"] * len(_TX_COLS))
    sql = (
        f"INSERT INTO transactions ({cols}) VALUES ({placeholders}) "
        "ON CONFLICT (transaction_id) DO NOTHING"
    )
    with conn.cursor() as cur:
        cur.execute(sql, _row_from_tx(tx))
        inserted = cur.rowcount == 1
    conn.commit()
    return inserted


def salvar_alerta(
    conn,
    transaction_id: str,
    regras: list[str],
    score: float,
    ml_prob: float | None = None,
) -> None:
    """Insere alerta ligado à transação."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO fraud_alerts "
            "(transaction_id, regras_ativadas, ml_probabilidade, score) "
            "VALUES (%s, %s, %s, %s)",
            (transaction_id, regras, ml_prob, score),
        )
    conn.commit()
