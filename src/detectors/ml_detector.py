"""Detector de fraudes baseado em Machine Learning."""

import pickle
from pathlib import Path
from typing import Any, Optional

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler


class MLDetector:
    """Detector de fraudes usando modelo de ML."""
    
    def __init__(self, modelo_path: Optional[str] = None) -> None:
        """Inicializa o detector ML.
        
        Args:
            modelo_path: Caminho para modelo treinado (None = usa modelo padrão)
        """
        self.modelo: Optional[RandomForestClassifier] = None
        self.scaler: Optional[StandardScaler] = None
        self.feature_names = [
            "valor",
            "hora",
            "dia_semana",
            "is_fim_semana",
            "latitude",
            "longitude",
            "dispositivo_encoded",
            "categoria_encoded",
        ]
        # Modelo v2 (real): [Time, V1..V28, Amount] = 30 dims
        self.feature_names_v2 = ["Time"] + [f"V{i}" for i in range(1, 29)] + ["Amount"]
        self.modelo_v2: Optional[RandomForestClassifier] = None
        self.scaler_v2: Optional[StandardScaler] = None
        
        if modelo_path and Path(modelo_path).exists():
            self._carregar_modelo(modelo_path)
        else:
            self._criar_modelo_padrao()
    
    def _criar_modelo_padrao(self) -> None:
        """Cria um modelo padrão para demonstração."""
        print("⚠️  Criando modelo padrão para demonstração")
        
        self.modelo = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=42,
            n_jobs=-1,
        )
        
        self.scaler = StandardScaler()
        
        # Gera dados sintéticos para treinar o modelo
        X_sintetico, y_sintetico = self._gerar_dados_sinteticos(1000)
        
        X_normalizado = self.scaler.fit_transform(X_sintetico)
        self.modelo.fit(X_normalizado, y_sintetico)
        
        print("✅ Modelo padrão criado e treinado")
    
    def _gerar_dados_sinteticos(
        self, n_samples: int
    ) -> tuple[np.ndarray, np.ndarray]:
        """Gera dados sintéticos para treinamento.
        
        Args:
            n_samples: Número de amostras
            
        Returns:
            Tuple de (features, labels)
        """
        np.random.seed(42)
        
        # Features: [valor, hora, dia_semana, is_fim_semana, lat, lon, dispositivo, categoria]
        X = np.zeros((n_samples, 8))
        
        # Transações normais (80%)
        n_normais = int(n_samples * 0.8)
        X[:n_normais, 0] = np.random.uniform(10, 2000, n_normais)  # valor baixo
        X[:n_normais, 1] = np.random.uniform(8, 22, n_normais)  # horário normal
        X[:n_normais, 2] = np.random.randint(0, 7, n_normais)
        X[:n_normais, 3] = np.random.choice([0, 1], n_normais, p=[0.7, 0.3])
        X[:n_normais, 4] = np.random.uniform(-33, 5, n_normais)  # lat BR
        X[:n_normais, 5] = np.random.uniform(-74, -34, n_normais)  # lon BR
        X[:n_normais, 6] = np.random.randint(0, 5, n_normais)
        X[:n_normais, 7] = np.random.randint(0, 10, n_normais)
        
        # Transações fraudulentas (20%)
        n_fraudes = n_samples - n_normais
        X[n_normais:, 0] = np.random.uniform(3000, 50000, n_fraudes)  # valor alto
        X[n_normais:, 1] = np.random.choice([0, 1, 2, 3, 4, 23], n_fraudes)  # horário incomum
        X[n_normais:, 2] = np.random.randint(0, 7, n_fraudes)
        X[n_normais:, 3] = np.random.choice([0, 1], n_fraudes, p=[0.3, 0.7])
        X[n_normais:, 4] = np.random.uniform(-33, 52, n_fraudes)  # lat global
        X[n_normais:, 5] = np.random.uniform(-74, 140, n_fraudes)  # lon global
        X[n_normais:, 6] = np.random.randint(0, 5, n_fraudes)
        X[n_normais:, 7] = np.random.randint(0, 10, n_fraudes)
        
        y = np.zeros(n_samples)
        y[n_normais:] = 1
        
        return X, y
    
    def treinar_com_csv(self, csv_path: str = "data/raw/creditcard.csv") -> dict[str, Any]:
        """Treina modelo v2 com o creditcard.csv real.

        Returns:
            Métricas básicas (precision/recall/f1 no holdout).
        """
        import pandas as pd
        from sklearn.metrics import f1_score, precision_score, recall_score
        from sklearn.model_selection import train_test_split

        df = pd.read_csv(csv_path)
        X = df[self.feature_names_v2].values
        y = df["Class"].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        self.scaler_v2 = StandardScaler()
        X_train_n = self.scaler_v2.fit_transform(X_train)
        X_test_n = self.scaler_v2.transform(X_test)

        self.modelo_v2 = RandomForestClassifier(
            n_estimators=200,
            max_depth=None,
            class_weight="balanced",
            random_state=42,
            n_jobs=-1,
        )
        self.modelo_v2.fit(X_train_n, y_train)
        y_pred = self.modelo_v2.predict(X_test_n)

        return {
            "precision": float(precision_score(y_test, y_pred, zero_division=0)),
            "recall": float(recall_score(y_test, y_pred, zero_division=0)),
            "f1": float(f1_score(y_test, y_pred, zero_division=0)),
            "n_train": int(len(y_train)),
            "n_test": int(len(y_test)),
        }

    def prever_v2(self, transacao: dict[str, Any]) -> dict[str, Any]:
        """Prevê usando features reais (Time, V1..V28, Amount)."""
        if self.modelo_v2 is None or self.scaler_v2 is None:
            raise ValueError("Modelo v2 não treinado. Chame treinar_com_csv().")

        # Aceita tanto dict achatado (V1..V28) quanto schema unificado
        if "v_features" in transacao and transacao["v_features"] is not None:
            vec = [transacao.get("time_original", 0.0)] + list(transacao["v_features"]) + [float(transacao["valor"])]
        else:
            vec = [float(transacao.get("Time", transacao.get("time_original", 0.0)))] + \
                  [float(transacao[f"V{i}"]) for i in range(1, 29)] + \
                  [float(transacao.get("Amount", transacao.get("valor")))]
        import numpy as _np
        X = _np.array(vec).reshape(1, -1)
        Xn = self.scaler_v2.transform(X)
        proba = float(self.modelo_v2.predict_proba(Xn)[0][1])
        return {
            "transaction_id": transacao.get("transaction_id", ""),
            "is_fraud": bool(proba >= 0.5),
            "probabilidade_fraude": proba,
            "modelo": "v2-real",
        }

    def _carregar_modelo(self, path: str) -> None:
        """Carrega modelo treinado de arquivo (v1 e/ou v2, o que houver).

        Args:
            path: Caminho do arquivo do modelo
        """
        with open(path, "rb") as f:
            dados = pickle.load(f)
            self.modelo = dados.get("modelo")
            self.scaler = dados.get("scaler")
            self.modelo_v2 = dados.get("modelo_v2")
            self.scaler_v2 = dados.get("scaler_v2")

        print(f"✅ Modelo carregado de {path}")

    def salvar_modelo(self, path: str) -> None:
        """Salva os modelos treinados em arquivo (v1 e/ou v2).

        Args:
            path: Caminho para salvar o modelo
        """
        if (self.modelo is None or self.scaler is None) and (
            self.modelo_v2 is None or self.scaler_v2 is None
        ):
            raise ValueError("Nenhum modelo treinado")

        Path(path).parent.mkdir(parents=True, exist_ok=True)

        with open(path, "wb") as f:
            pickle.dump({
                "modelo": self.modelo,
                "scaler": self.scaler,
                "modelo_v2": self.modelo_v2,
                "scaler_v2": self.scaler_v2,
            }, f)

        print(f"✅ Modelo salvo em {path}")
    
    def _extrair_features(self, transacao: dict[str, Any]) -> np.ndarray:
        """Extrai features de uma transação.
        
        Args:
            transacao: Dados da transação
            
        Returns:
            Array de features
        """
        from datetime import datetime
        
        timestamp = datetime.fromisoformat(transacao["timestamp"])
        
        # Mapeamento simples de dispositivos e categorias
        dispositivos = {"mobile": 0, "desktop": 1, "tablet": 2, "pos": 3, "atm": 4}
        categorias = {
            "alimentacao": 0, "transporte": 1, "saude": 2, "educacao": 3,
            "lazer": 4, "compras": 5, "servicos": 6, "transferencia": 7,
            "saque": 8, "deposito": 9,
        }
        
        local = transacao.get("localizacao", {})
        
        features = np.array([
            transacao["valor"],
            timestamp.hour,
            timestamp.weekday(),
            1 if timestamp.weekday() >= 5 else 0,
            local.get("latitude", 0),
            local.get("longitude", 0),
            dispositivos.get(transacao.get("dispositivo", ""), 0),
            categorias.get(transacao.get("categoria", ""), 0),
        ]).reshape(1, -1)
        
        return features
    
    def prever(self, transacao: dict[str, Any]) -> dict[str, Any]:
        """Prevê se uma transação é fraudulenta.
        
        Args:
            transacao: Dados da transação
            
        Returns:
            Dict com previsão e probabilidade
        """
        if self.modelo is None or self.scaler is None:
            raise ValueError("Modelo não inicializado")
        
        features = self._extrair_features(transacao)
        features_normalizadas = self.scaler.transform(features)
        
        previsao = self.modelo.predict(features_normalizadas)[0]
        probabilidades = self.modelo.predict_proba(features_normalizadas)[0]
        
        prob_fraude = float(probabilidades[1])
        
        return {
            "transaction_id": transacao["transaction_id"],
            "is_fraud": bool(previsao),
            "probabilidade_fraude": prob_fraude,
            "confianca": float(max(probabilidades)),
            "features_usadas": dict(zip(self.feature_names, features[0].tolist())),
        }
    
    def obter_importancia_features(self) -> dict[str, float]:
        """Retorna importância das features no modelo."""
        if self.modelo is None:
            return {}
        
        importancias = self.modelo.feature_importances_
        return dict(zip(self.feature_names, importancias.tolist()))


def main() -> None:
    """Função principal para demonstração."""
    print("🤖 Iniciando Detector ML...")
    
    detector = MLDetector()
    
    # Exemplo de transação
    transacao_exemplo = {
        "transaction_id": "ml-001",
        "user_id": 42,
        "valor": 15000.00,
        "timestamp": "2024-01-15T03:30:00",
        "localizacao": {
            "cidade": "Nova York",
            "pais": "US",
            "latitude": 40.71,
            "longitude": -74.01,
        },
        "dispositivo": "mobile",
        "categoria": "transferencia",
    }
    
    print("\n🔍 Analisando transação...")
    resultado = detector.prever(transacao_exemplo)
    
    print(f"   ID: {resultado['transaction_id']}")
    print(f"   É fraude: {'🚨 SIM' if resultado['is_fraud'] else '✅ NÃO'}")
    print(f"   Probabilidade: {resultado['probabilidade_fraude']:.2%}")
    print(f"   Confiança: {resultado['confianca']:.2%}")
    
    print("\n📊 Importância das features:")
    importancia = detector.obter_importancia_features()
    for feature, valor in sorted(importancia.items(), key=lambda x: x[1], reverse=True):
        print(f"   {feature}: {valor:.4f}")


if __name__ == "__main__":
    main()
