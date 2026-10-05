"""Testes do RuleEngine (inclui regressão do bug geografico)."""

from datetime import datetime, timezone

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


def test_valor_alto_dispara():
    eng = RuleEngine()
    # dispositivo já conhecido p/ isolar só valor_alto
    eng.dispositivos_conhecidos[42].add("mobile")
    alerta = eng.verificar_transacao(tx_base(valor=15000.0))
    assert alerta is not None
    assert "valor_alto" in alerta.regras_ativadas


def test_valor_normal_nao_dispara_valor():
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    alerta = eng.verificar_transacao(tx_base(valor=50.0))
    # horario 12h não é incomum; sem velocidade/geográfico -> sem alerta
    assert alerta is None


def test_horario_incomum_dispara():
    eng = RuleEngine()
    eng.dispositivos_conhecidos[42].add("mobile")
    alerta = eng.verificar_transacao(tx_base(timestamp="2013-09-01T03:30:00+00:00"))
    assert alerta is not None
    assert "horario_incomum" in alerta.regras_ativadas


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


def test_dispositivo_novo_primeira_vez():
    eng = RuleEngine()
    primeira = eng.verificar_transacao(tx_base(
        timestamp="2013-09-01T12:00:00+00:00", dispositivo="tablet-novo-xyz"))
    assert primeira is not None
    assert "dispositivo_novo" in primeira.regras_ativadas
    # segunda vez com mesmo dispositivo não dispara mais
    segunda = eng.verificar_transacao(tx_base(
        transaction_id="tx-2",
        timestamp="2013-09-01T13:00:00+00:00", dispositivo="tablet-novo-xyz"))
    regras = segunda.regras_ativadas if segunda else []
    assert "dispositivo_novo" not in regras


def test_score_soma_pesos():
    eng = RuleEngine()
    pesos = {r.nome: r.peso for r in eng.regras}
    alerta = eng.verificar_transacao(tx_base(
        valor=20000.0, timestamp="2013-09-01T03:00:00+00:00"))
    assert alerta is not None
    esperado = sum(pesos[r] for r in alerta.regras_ativadas)
    assert alerta.score == esperado
