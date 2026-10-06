"""Testes do schema unificado Transacao (sem Kafka/DB, sem CSV de 144MB)."""

import pytest

from src.schemas.transaction import Transacao


def linha_csv_fraud() -> dict:
    """Linha sintética no formato creditcard.csv."""
    row = {"Time": "100.0", "Amount": "15000.00", "Class": "1"}
    row.update({f"V{i}": f"{i * 0.1:.2f}" for i in range(1, 29)})
    return row


def linha_csv_normal() -> dict:
    row = {"Time": "200.0", "Amount": "25.50", "Class": "0"}
    row.update({f"V{i}": "0.0" for i in range(1, 29)})
    return row


def test_from_creditcard_row_mapeia_campos():
    tx = Transacao.from_creditcard_row(linha_csv_fraud(), idx=0)
    assert tx.valor == pytest.approx(15000.00)
    assert tx.is_fraud is True
    assert tx.source == "creditcard_csv"
    assert tx.time_original == pytest.approx(100.0)
    assert len(tx.v_features) == 28
    assert tx.validar() == []


def test_from_creditcard_row_normal():
    tx = Transacao.from_creditcard_row(linha_csv_normal(), idx=5)
    assert tx.is_fraud is False
    assert tx.user_id == 6  # idx % 1000 + 1


def test_to_dict_achata_v_features():
    tx = Transacao.from_creditcard_row(linha_csv_fraud(), idx=0)
    d = tx.to_dict()
    assert d["V1"] == pytest.approx(0.1)
    assert d["V28"] == pytest.approx(2.8)
    assert len(d["v_features"]) == 28


def test_roundtrip_from_dict():
    tx = Transacao.from_creditcard_row(linha_csv_fraud(), idx=3)
    rt = Transacao.from_dict(tx.to_dict())
    assert rt.valor == tx.valor
    assert rt.is_fraud == tx.is_fraud
    assert rt.v_features == pytest.approx(tx.v_features)
    assert rt.validar() == []


def test_to_ml_vector_v2_tem_30_dims():
    tx = Transacao.from_creditcard_row(linha_csv_fraud(), idx=0)
    vec = tx.to_ml_vector_v2()
    assert len(vec) == 30  # Time + V1..V28 + Amount
    assert vec[0] == pytest.approx(100.0)
    assert vec[-1] == pytest.approx(15000.00)


def test_sintetica_sem_v_features_erro_v2():
    tx = Transacao(
        transaction_id="x", user_id=1, valor=10.0,
        timestamp="2024-01-01T10:00:00", source="synthetic",
    )
    with pytest.raises(ValueError):
        tx.to_ml_vector_v2()


def test_validar_aponta_erros():
    tx = Transacao(
        transaction_id="", user_id=0, valor=-5.0,
        timestamp="nao-e-data", v_features=[1.0, 2.0],
    )
    erros = tx.validar()
    assert len(erros) >= 4


def test_replay_id_deterministico():
    """R1: mesma linha do CSV gera sempre o mesmo ID (re-replay deduplica)."""
    a = Transacao.from_creditcard_row(linha_csv_fraud(), idx=0)
    b = Transacao.from_creditcard_row(linha_csv_fraud(), idx=999)
    assert a.transaction_id == b.transaction_id
    c = Transacao.from_creditcard_row(linha_csv_normal(), idx=0)
    assert a.transaction_id != c.transaction_id


def test_from_dict_normaliza_naive_para_utc():
    """R3: timestamp naive vira aware UTC na fronteira."""
    from datetime import datetime

    tx = Transacao.from_dict({
        "transaction_id": "t1", "user_id": 1, "valor": 10.0,
        "timestamp": "2024-01-01T10:00:00",  # naive, estilo sintético antigo
    })
    assert datetime.fromisoformat(tx.timestamp).tzinfo is not None
    assert tx.validar() == []
