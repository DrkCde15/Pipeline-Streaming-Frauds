"""Treina o ML v2 no creditcard.csv e salva o artefato para o serving.

Uso (da raiz do repo):
    python scripts/train_ml.py [caminho_csv] [saida_pkl]

O consumer carrega `models/fraud_rf_v2.pkl` no boot (R2); sem o arquivo
ele tenta treinar sozinho (lento) ou segue só com regras.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

CSV_DEFAULT = "data/raw/creditcard.csv"
MODEL_DEFAULT = "models/fraud_rf_v2.pkl"


def main(csv_path: str = CSV_DEFAULT, out_path: str = MODEL_DEFAULT) -> None:
    if not Path(csv_path).exists():
        sys.exit(
            f"CSV não encontrado: {csv_path}\n"
            "Baixe com: kaggle datasets download mlg-ulb/creditcardfraud "
            "-p data/raw --unzip"
        )

    from src.detectors.ml_detector import MLDetector

    print(f"Treinando v2 com {csv_path} ...")
    det = MLDetector()
    metricas = det.treinar_com_csv(csv_path)
    det.salvar_modelo(out_path)
    print(f"Métricas: {metricas}")
    print(f"Artefato: {out_path}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
