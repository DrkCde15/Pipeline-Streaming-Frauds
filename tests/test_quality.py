"""Testes dos checks de qualidade (puros + integração opcional)."""

import pytest

from scripts.check_quality import avaliar_drift


def test_drift_ok():
    st, _ = avaliar_drift(100.0, 110.0, 30.0)
    assert st == "OK"


def test_drift_warn_na_metade_do_teto():
    st, _ = avaliar_drift(100.0, 120.0, 30.0)
    assert st == "WARN"


def test_drift_fail_acima_do_teto():
    st, det = avaliar_drift(100.0, 200.0, 30.0)
    assert st == "FAIL"
    assert "100.0%" in det


def test_drift_baseline_zerada():
    st, _ = avaliar_drift(0.0, 50.0, 30.0)
    assert st == "WARN"


def test_checks_no_banco():
    """Exige Postgres no ar; pula sem DB. Não limpa tabelas (só usa fixture).

    Insere 2 linhas fixture com IDs próprios, valida que elas passam
    limpas nos checks, e as remove ao final. Nunca dá TRUNCATE/DELETE amplo:
    o writer commita por insert, então nada aqui pode depender de rollback.
    """
    try:
        from src.schemas.transaction import Transacao
        from src.storage.postgres_writer import get_conn, salvar_transacao

        conn = get_conn()
    except Exception as e:
        pytest.skip(f"Postgres indisponível: {e}")
    try:
        from scripts.check_quality import rodar_checks  # noqa: F811

        def linha(time: str, amount: str) -> dict:
            row = {"Time": time, "Amount": amount, "Class": "0"}
            row.update({f"V{i}": "0.1" for i in range(1, 29)})
            return row

        ids = []
        for idx, (t, v) in enumerate([("50.0", "99.90"), ("60.0", "10.00")]):
            tx = Transacao.from_creditcard_row(linha(t, v), idx=idx).to_dict()
            salvar_transacao(conn, tx)
            ids.append(tx["transaction_id"])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM transactions WHERE transaction_id = ANY(%s::uuid[]) "
                "AND (user_id IS NULL OR valor IS NULL OR timestamp IS NULL)",
                (ids,),
            )
            assert cur.fetchone()[0] == 0
            cur.execute(
                "SELECT COUNT(*) FROM (SELECT time_original, valor, user_id "
                "FROM transactions WHERE transaction_id = ANY(%s::uuid[]) "
                "GROUP BY 1, 2, 3 HAVING COUNT(*) > 1) d",
                (ids,),
            )
            assert cur.fetchone()[0] == 0
        res = {r.nome: r for r in rodar_checks(conn, 30.0, 5.0)}
        assert set(res) == {
            "nulos",
            "duplicadas",
            "drift_amount",
            "taxa_fraude",
            "valores_impossiveis",
            "alertas_orfaos",
        }
        assert all(r.status in ("OK", "WARN", "FAIL") for r in res.values())
    finally:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM transactions WHERE transaction_id = ANY(%s::uuid[])", (ids,))
        conn.commit()
        conn.close()
