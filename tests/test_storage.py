"""Testes do writer Postgres (mapping puro + integração opcional)."""

import pytest

from src.schemas.transaction import Transacao
from src.storage.postgres_writer import _row_from_tx


def linha_csv() -> dict:
    row = {"Time": "50.0", "Amount": "99.90", "Class": "0"}
    row.update({f"V{i}": "0.5" for i in range(1, 29)})
    return row


def test_row_tem_41_colunas_na_ordem():
    tx = Transacao.from_creditcard_row(linha_csv(), idx=1).to_dict()
    vals = _row_from_tx(tx)
    assert len(vals) == 13 + 28
    assert vals[0] == tx["transaction_id"]
    assert vals[1] == tx["user_id"]
    assert vals[5] == "BR"  # pais default (CSV anonimizado)
    assert vals[13:] == tuple([0.5] * 28)


def test_integracao_banco():
    """Exige Postgres no ar (compose). Pula sem DB para CI sem infra."""
    try:
        from src.storage.postgres_writer import (
            get_conn,
            salvar_alerta,
            salvar_transacao,
        )

        conn = get_conn()
    except Exception as e:
        pytest.skip(f"Postgres indisponível: {e}")
    try:
        tx = Transacao.from_creditcard_row(linha_csv(), idx=999).to_dict()
        assert salvar_transacao(conn, tx) is True
        assert salvar_transacao(conn, tx) is False  # idempotente
        salvar_alerta(conn, tx["transaction_id"], ["valor_alto"], 1.5)
    finally:
        # Limpa p/ o teste ser repetível (R1: ID é determinístico por linha)
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM fraud_alerts WHERE transaction_id = %s",
                (Transacao.from_creditcard_row(linha_csv(), idx=999).to_dict()["transaction_id"],),
            )
            cur.execute(
                "DELETE FROM transactions WHERE transaction_id = %s",
                (Transacao.from_creditcard_row(linha_csv(), idx=999).to_dict()["transaction_id"],),
            )
        conn.commit()
        conn.close()
