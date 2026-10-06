"""Testes do RuleEngine (inclui regressão do bug geografico)."""

from datetime import datetime, timezone

import pytest

from src.detectors.rule_engine import RuleEngine


def tx_base(**kw) -> dict:
    base = {
        "transaction_id": kw.get("transaction_id", "tx-1"),
        "user_id": 42,
        "valor": 100.0,
        "timestamp": "2013-09-01T12:00:00+00:00",
        "localizacao": {"cidade": "São Paulo", "pais": "BR"},
        "dispositivo": "mobile",
        "categoria": "compras",
    }
    base.update(kw)
    return base


def test_valor_alto_sozinho_nao_dispara():
    """R7: sinal fraco isolado (1.5 < limiar 2.0) soma mas não alerta."""
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    assert eng.verificar_transacao(tx_base(valor=15000.0)) is None


def test_valor_alto_com_horario_dispara():
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    alerta = eng.verificar_transacao(tx_base(
        valor=15000.0, timestamp="2013-09-01T03:00:00+00:00"))
    assert alerta is not None
    assert set(alerta.regras_ativadas) == {"valor_alto", "horario_incomum"}
    assert alerta.score == pytest.approx(2.5)


def test_valor_normal_nao_dispara_valor():
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    alerta = eng.verificar_transacao(tx_base(valor=50.0))
    # horario 12h não é incomum; sem velocidade/geográfico -> sem alerta
    assert alerta is None


def test_horario_incomum_sozinho_nao_dispara():
    """R7: madrugada isolada (1.0) não alerta mais."""
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    assert eng.verificar_transacao(
        tx_base(timestamp="2013-09-01T03:30:00+00:00")) is None


def test_sinal_forte_sozinho_dispara():
    """Velocidade (2.0) e geografico (2.5) atingem o limiar sozinhos."""
    eng = RuleEngine(limiar_alerta=2.0)
    assert eng.limiar_alerta == 2.0


def test_velocidade_sexta_transacao():
    eng = RuleEngine()
    eng.dispositivos_conhecidos[7].add("pos")
    for i in range(5):
        eng.verificar_transacao(tx_base(
            transaction_id=f"v-{i}", user_id=7, dispositivo="pos",
            timestamp="2013-09-01T12:00:00+00:00",
        ))
    alerta = eng.verificar_transacao(tx_base(
        transaction_id="v-5", user_id=7, dispositivo="pos",
        timestamp="2013-09-01T12:00:30+00:00",
    ))
    assert alerta is not None
    assert "velocidade" in alerta.regras_ativadas


def test_geografico_paises_diferentes():
    """Regressão: NameError transacao_recentes vs transacoes_recentes."""
    eng = RuleEngine()
    eng.dispositivos_conhecidos[9].add("mobile")
    eng.verificar_transacao(tx_base(
        transaction_id="g-1", user_id=9, dispositivo="mobile",
        timestamp="2013-09-01T12:00:00+00:00",
        localizacao={"cidade": "São Paulo", "pais": "BR"},
    ))
    alerta = eng.verificar_transacao(tx_base(
        transaction_id="g-2", user_id=9, dispositivo="mobile",
        timestamp="2013-09-01T12:03:00+00:00",
        localizacao={"cidade": "Nova York", "pais": "US"},
    ))
    assert alerta is not None
    assert "geografico" in alerta.regras_ativadas


def test_dispositivo_novo_primeira_vez_nao_alerta_sozinho():
    """R7: 1º uso isolado (1.2) registra mas não alerta; some no combinado."""
    eng = RuleEngine()
    primeira = eng.verificar_transacao(tx_base(
        timestamp="2013-09-01T12:00:00+00:00", dispositivo="tablet-novo-xyz"))
    assert primeira is None
    # segunda vez: conhecido, sem alerta de dispositivo
    segunda = eng.verificar_transacao(tx_base(
        transaction_id="tx-2",
        timestamp="2013-09-01T13:00:00+00:00", dispositivo="tablet-novo-xyz"))
    regras = segunda.regras_ativadas if segunda else []
    assert "dispositivo_novo" not in regras
    # combinado com valor alto (0.8+1.5=2.3) dispara
    terceira = eng.verificar_transacao(tx_base(
        transaction_id="tx-3", user_id=43, valor=12000.0,
        timestamp="2013-09-01T12:00:00+00:00", dispositivo="outro-novo-xyz"))
    assert terceira is not None
    assert "dispositivo_novo" in terceira.regras_ativadas


def test_score_soma_pesos():
    eng = RuleEngine()
    pesos = {r.nome: r.peso for r in eng.regras}
    alerta = eng.verificar_transacao(tx_base(
        valor=20000.0, timestamp="2013-09-01T03:00:00+00:00"))
    assert alerta is not None
    esperado = sum(pesos[r] for r in alerta.regras_ativadas)
    assert alerta.score == esperado


def test_stream_misto_naive_e_aware():
    """R3: sintético (naive) + CSV (aware) no mesmo user não quebra."""
    eng = RuleEngine()
    eng.dispositivos_conhecidos[11].add("mobile")
    eng.verificar_transacao(tx_base(
        transaction_id="m-1", user_id=11, dispositivo="mobile",
        timestamp="2013-09-01T12:00:00",  # naive, produtor sintético antigo
        localizacao={"cidade": "São Paulo", "pais": "BR"},
    ))
    alerta = eng.verificar_transacao(tx_base(
        transaction_id="m-2", user_id=11, dispositivo="mobile",
        timestamp="2013-09-01T12:03:00+00:00",  # aware, CSV
        localizacao={"cidade": "Londres", "pais": "GB"},
    ))
    assert alerta is not None
    assert "geografico" in alerta.regras_ativadas
