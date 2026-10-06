"""Testes R2: combinação regras+ML e persistência do artefato v2."""

import numpy as np
import pytest
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

from src.detectors.ml_detector import MLDetector
from src.detectors.rule_engine import decidir_alerta


def test_regras_sozinhas_disparam():
    dispara, score = decidir_alerta(["valor_alto"], 1.5, None)
    assert dispara is True
    assert score == 1.5


def test_ml_sozinho_dispara():
    dispara, score = decidir_alerta([], 0.0, 0.9)
    assert dispara is True
    assert score == pytest.approx(0.9)


def test_ml_baixo_nao_dispara():
    dispara, _ = decidir_alerta([], 0.0, 0.2)
    assert dispara is False


def test_nada_nao_dispara():
    dispara, score = decidir_alerta([], 0.0, None)
    assert dispara is False
    assert score == 0.0


def test_limiar_custom():
    dispara, _ = decidir_alerta([], 0.0, 0.7, limiar_ml=0.8)
    assert dispara is False
    dispara, _ = decidir_alerta([], 0.0, 0.9, limiar_ml=0.8)
    assert dispara is True


def _det_treino_mini() -> MLDetector:
    """RF v2 mínimo em dados sintéticos (rápido, sem CSV de 144MB)."""
    rng = np.random.RandomState(7)
    X = rng.uniform(-3, 3, size=(80, 30))
    y = (X[:, 0] + X[:, 5] > 1.0).astype(int)
    det = MLDetector()
    det.scaler_v2 = StandardScaler().fit(X)
    det.modelo_v2 = RandomForestClassifier(n_estimators=5, random_state=7)
    det.modelo_v2.fit(det.scaler_v2.transform(X), y)
    return det


def test_artefato_v2_roundtrip(tmp_path):
    det = _det_treino_mini()
    pkl = str(tmp_path / "mini.pkl")
    det.salvar_modelo(pkl)

    recarregado = MLDetector()
    recarregado._carregar_modelo(pkl)
    assert recarregado.modelo_v2 is not None

    tx = {"transaction_id": "t", "valor": 10.0, "time_original": 1.0, "v_features": [0.0] * 30}
    tx["v_features"] = [5.0] + [0.0] * 27  # força classe 1 no treino mini
    r = recarregado.prever_v2(tx)
    assert 0.0 <= r["probabilidade_fraude"] <= 1.0
    assert r["modelo"] == "v2-real"


def test_salvar_sem_treino_erro(tmp_path):
    vazio = MLDetector.__new__(MLDetector)
    vazio.modelo = vazio.scaler = None
    vazio.modelo_v2 = vazio.scaler_v2 = None
    with pytest.raises(ValueError):
        vazio.salvar_modelo(str(tmp_path / "nunca.pkl"))
