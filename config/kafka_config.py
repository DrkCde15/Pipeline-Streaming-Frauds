"""Configurações do Kafka (lê .env / ambiente, com defaults locais)."""

import os
from dataclasses import dataclass, field


def _load_dotenv(path: str = ".env") -> None:
    """Carrega .env simples sem dependência externa (KEY=VALUE, ignora #)."""
    if os.path.exists(path) and "KAFKA_BOOTSTRAP" not in os.environ:
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())
        except OSError:
            pass


_load_dotenv()


@dataclass
class KafkaConfig:
    """Configurações de conexão com Kafka."""

    bootstrap_servers: list[str] = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
    )
    topic_transactions: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")
    )
    topic_fraud_alerts: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC_FRAUD_ALERTS", "fraud-alerts")
    )
    group_id: str = field(
        default_factory=lambda: os.getenv("KAFKA_GROUP_ID", "fraud-detection-group")
    )
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False

    @property
    def bootstrap_servers_str(self) -> str:
        """Retorna bootstrap servers como string."""
        return ",".join(self.bootstrap_servers)


KAFKA_CONFIG = KafkaConfig()
