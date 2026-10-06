"""Checks de qualidade no Postgres (v0.3).

Uso (da raiz do repo):
    python scripts/check_quality.py

Exit 0 = tudo ok (warnings não quebram); exit 1 = alguma falha.
Limiares via env: DQ_MAX_DRIFT_PCT (default 30), DQ_MAX_FRAUD_PCT (default 5).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@dataclass
class Resultado:
    nome: str
    status: str  # OK | WARN | FAIL
    detalhe: str


def avaliar_drift(media_base: float, media_atual: float, max_pct: float) -> tuple[str, str]:
    """Compara médias (puro, testável sem DB)."""
    if media_base == 0:
        return ("WARN", "baseline zerada, sem referência")
    var = abs(media_atual - media_base) / abs(media_base) * 100
    detalhe = f"baseline={media_base:.2f} atual={media_atual:.2f} var={var:.1f}%"
    if var > max_pct:
        return ("FAIL", detalhe)
    if var > max_pct / 2:
        return ("WARN", detalhe)
    return ("OK", detalhe)


def rodar_checks(conn, max_drift_pct: float, max_fraud_pct: float) -> list[Resultado]:
    out: list[Resultado] = []
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM transactions")
        total = cur.fetchone()[0]
        if total == 0:
            return [Resultado("volume", "WARN", "transactions vazia — nada a checar")]

        # 1. Nulos em colunas críticas (schema diz NOT NULL; confirma dado real)
        cur.execute(
            "SELECT COUNT(*) FROM transactions WHERE "
            "transaction_id IS NULL OR user_id IS NULL "
            "OR valor IS NULL OR timestamp IS NULL"
        )
        n = cur.fetchone()[0]
        out.append(Resultado(
            "nulos", "FAIL" if n else "OK", f"{n} linhas com nulo crítico"))

        # 2. Duplicadas lógicas (mesmo evento natural, IDs diferentes)
        cur.execute(
            "SELECT COUNT(*) FROM (SELECT time_original, valor, user_id "
            "FROM transactions WHERE source = 'creditcard_csv' "
            "GROUP BY 1, 2, 3 HAVING COUNT(*) > 1) d"
        )
        n = cur.fetchone()[0]
        out.append(Resultado(
            "duplicadas", "FAIL" if n else "OK",
            f"{n} grupos (time,valor,user) repetidos"))

        # 3. Drift de Amount na fonte real (dia 1 como baseline vs dia 2).
        # Restrito a creditcard_csv: o sintético usa outra escala e outro
        # período, então misturar as fontes geraria falso drift.
        cur.execute(
            "SELECT AVG(valor)::float FROM transactions "
            "WHERE source = 'creditcard_csv' "
            "AND timestamp < '2013-09-02T00:00:00+00:00'"
        )
        base = cur.fetchone()[0]
        cur.execute(
            "SELECT AVG(valor)::float FROM transactions "
            "WHERE source = 'creditcard_csv' "
            "AND timestamp >= '2013-09-02T00:00:00+00:00'"
        )
        atual = cur.fetchone()[0]
        if base is None or atual is None:
            lado = "dia 1" if base is None else "dia 2"
            out.append(Resultado(
                "drift_amount", "WARN",
                f"sem linhas creditcard_csv no {lado} (replay cobre ~33min do dia 1)"))
        else:
            st, det = avaliar_drift(base, atual, max_drift_pct)
            out.append(Resultado("drift_amount", st, det))

        # 4. Taxa de fraude dentro do plausível (0–max%)
        cur.execute(
            "SELECT 100.0 * SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)"
            " / NULLIF(COUNT(*), 0) FROM transactions"
        )
        taxa = cur.fetchone()[0] or 0.0
        st = "FAIL" if taxa > max_fraud_pct else "OK"
        out.append(Resultado("taxa_fraude", st, f"{taxa:.3f}% (teto {max_fraud_pct}%)"))

        # 5. Valores impossíveis
        cur.execute(
            "SELECT COUNT(*) FROM transactions WHERE valor < 0 "
            "OR timestamp > NOW() + INTERVAL '1 day'"
        )
        n = cur.fetchone()[0]
        out.append(Resultado(
            "valores_impossiveis", "FAIL" if n else "OK",
            f"{n} linhas (valor<0 ou timestamp futuro)"))

        # 6. Alertas órfãos (FK deveria impedir; confirma)
        cur.execute(
            "SELECT COUNT(*) FROM fraud_alerts fa LEFT JOIN transactions t "
            "ON t.transaction_id = fa.transaction_id WHERE t.transaction_id IS NULL"
        )
        n = cur.fetchone()[0]
        out.append(Resultado(
            "alertas_orfaos", "FAIL" if n else "OK", f"{n} alertas sem transação"))
    return out


def main() -> int:
    from src.storage.postgres_writer import get_conn

    max_drift = float(os.getenv("DQ_MAX_DRIFT_PCT", "30"))
    max_fraud = float(os.getenv("DQ_MAX_FRAUD_PCT", "5"))
    try:
        conn = get_conn()
    except Exception as e:
        print(f"FAIL conexao: {e}")
        return 1

    resultados = rodar_checks(conn, max_drift, max_fraud)
    conn.close()
    for r in resultados:
        print(f"{r.status:4} {r.nome:20} {r.detalhe}")
    return 1 if any(r.status == "FAIL" for r in resultados) else 0


if __name__ == "__main__":
    raise SystemExit(main())
