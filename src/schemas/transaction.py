"""Schema unificado de transações.

Une o schema de negócio original (streaming simulado) com o
dataset real creditcard.csv (ULB / Worldline):
  CSV: Time, V1..V28, Amount, Class

Mantém compatibilidade com:
- RuleEngine (precisa de user_id, timestamp, valor, localizacao, dispositivo)
- MLDetector v1 (8 features sintéticas) e v2 (30 features reais)
- Dashboard Grafana (categoria, dispositivo)
- PostgreSQL (ver db/schema.sql)
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

# Base temporal do dataset ULB: 2 dias de setembro/2013.
# O campo `Time` do CSV são segundos decorridos desde a 1ª transação.
BASE_TIME_ULB = datetime(2013, 9, 1, 0, 0, 0, tzinfo=timezone.utc)

V_FEATURES = [f"V{i}" for i in range(1, 29)]  # V1..V28

DISPOSITIVOS_FALLBACK = ["mobile", "desktop", "tablet", "pos", "atm"]
CATEGORIAS_FALLBACK = [
    "alimentacao", "transporte", "saude", "educacao", "lazer",
    "compras", "servicos", "transferencia", "saque", "deposito",
]


def normalizar_timestamp(ts: str) -> str:
    """Garante ISO8601 com timezone (naive assume UTC).

    Fronteira anti-mistura naive/aware (R3): o sintético gera naive e o
    CSV gera aware; sem isso o RuleEngine levanta TypeError no stream misto.
    """
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


@dataclass
class Localizacao:
    """Localização da transação."""

    cidade: str = "Desconhecida"
    pais: str = "BR"
    latitude: float = 0.0
    longitude: float = 0.0


@dataclass
class Transacao:
    """Schema canônico de uma transação no pipeline.

    Campos de negócio (sempre presentes) + campos ML reais (quando
    source == 'creditcard_csv').
    """

    transaction_id: str
    user_id: int
    valor: float  # = Amount do CSV
    timestamp: str  # ISO8601 UTC
    is_fraud: bool = False
    moeda: str = "BRL"
    localizacao: Localizacao = field(default_factory=Localizacao)
    dispositivo: str = "unknown"
    categoria: str = "outros"
    # Proveniência: 'synthetic' | 'creditcard_csv'
    source: str = "synthetic"
    # Campos originais do CSV (None quando sintético)
    time_original: float | None = None
    v_features: list[float] | None = None  # 28 floats V1..V28

    def to_dict(self) -> dict[str, Any]:
        """Serializa para Kafka / PostgreSQL (JSON-ready)."""
        d = asdict(self)
        # Achata v_features em V1..V28 para facilitar SQL/Grafana,
        # mas mantém também a lista.
        if self.v_features is not None:
            for i, v in enumerate(self.v_features, start=1):
                d[f"V{i}"] = v
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Transacao":
        """Desserializa (Kafka consumer / DB row). Tolera ambos formatos."""
        loc = d.get("localizacao", {}) or {}
        localizacao = Localizacao(
            cidade=loc.get("cidade", "Desconhecida"),
            pais=loc.get("pais", "BR"),
            latitude=float(loc.get("latitude", 0.0)),
            longitude=float(loc.get("longitude", 0.0)),
        )
        # Reconstrói v_features a partir de V1..V28 ou da lista
        v_features = d.get("v_features")
        if v_features is None and any(f"V{i}" in d for i in range(1, 29)):
            try:
                v_features = [float(d[f"V{i}"]) for i in range(1, 29)]
            except (KeyError, TypeError, ValueError):
                v_features = None

        return cls(
            transaction_id=str(d["transaction_id"]),
            user_id=int(d["user_id"]),
            valor=float(d["valor"]),
            timestamp=normalizar_timestamp(str(d["timestamp"])),
            is_fraud=bool(d.get("is_fraud", False)),
            moeda=str(d.get("moeda", "BRL")),
            localizacao=localizacao,
            dispositivo=str(d.get("dispositivo", "unknown")),
            categoria=str(d.get("categoria", "outros")),
            source=str(d.get("source", "synthetic")),
            time_original=d.get("time_original"),
            v_features=v_features,
        )

    @classmethod
    def from_creditcard_row(
        cls,
        row: dict[str, Any],
        idx: int = 0,
        base_time: datetime = BASE_TIME_ULB,
    ) -> "Transacao":
        """Converte uma linha do creditcard.csv para o schema canônico.

        Args:
            row: dict com Time, V1..V28, Amount, Class
            idx: índice da linha (usado p/ derivar user_id/dispositivo
                 deterministicamente, permitindo testar regra de velocidade)
            base_time: origem do campo Time
        """
        time_sec = float(row["Time"])
        amount = float(row["Amount"])
        is_fraud = bool(int(row["Class"]))
        v_features = [float(row[f"V{i}"]) for i in range(1, 29)]

        if base_time.tzinfo is None:
            base_time = base_time.replace(tzinfo=timezone.utc)
        ts = (base_time + timedelta(seconds=time_sec)).isoformat()

        # ID determinístico do conteúdo: re-replay da mesma linha gera o
        # mesmo transaction_id, então o ON CONFLICT do Postgres + o
        # group_id do Kafka deduplicam em vez de inflar counts (R1).
        chave = "|".join([
            str(row["Time"]), str(row["Amount"]), str(row["Class"]),
            *[str(row[f"V{i}"]) for i in range(1, 29)],
        ])
        tx_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"creditcard_csv:{chave}"))

        # CSV não tem user: agrupa de 200 em 200 p/ permitir
        # testar velocidade/geográfico sem pulverizar em 284k usuários.
        user_id = (idx % 1000) + 1

        # Campos de negócio não existem no CSV -> preenchimento
        # determinístico (não aleatório) para replay reproduzível.
        # ML real NÃO usa esses campos, só RuleEngine/dashboard.
        dispositivo = DISPOSITIVOS_FALLBACK[idx % len(DISPOSITIVOS_FALLBACK)]
        categoria = CATEGORIAS_FALLBACK[idx % len(CATEGORIAS_FALLBACK)]

        return cls(
            transaction_id=tx_id,
            user_id=user_id,
            valor=amount,
            timestamp=ts,
            is_fraud=is_fraud,
            moeda="BRL",
            localizacao=Localizacao(),  # CSV é anonimizado (PCA), sem geo real
            dispositivo=dispositivo,
            categoria=categoria,
            source="creditcard_csv",
            time_original=time_sec,
            v_features=v_features,
        )

    def to_ml_vector_v2(self) -> list[float]:
        """Vetor real p/ modelo: [Time, V1..V28, Amount] = 30 dims.

        Raises:
            ValueError: se transação sintética sem v_features.
        """
        if self.v_features is None or self.time_original is None:
            raise ValueError(
                "Transação sem features reais (sintética?). "
                "Use to_ml_vector_v1() ou treine com CSV."
            )
        return [self.time_original] + self.v_features + [self.valor]

    def validar(self) -> list[str]:
        """Validação leve. Retorna lista de erros (vazia = válida)."""
        erros: list[str] = []
        if not self.transaction_id:
            erros.append("transaction_id vazio")
        if self.user_id < 1:
            erros.append("user_id inválido")
        if self.valor < 0:
            erros.append("valor negativo")
        try:
            datetime.fromisoformat(self.timestamp)
        except ValueError:
            erros.append("timestamp inválido (esperado ISO8601)")
        if self.v_features is not None and len(self.v_features) != 28:
            erros.append(f"v_features com {len(self.v_features)} itens, esperado 28")
        return erros
