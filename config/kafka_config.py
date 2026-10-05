"""Configurações do Kafka."""

from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    """Configurações de conexão com Kafka."""
    
    bootstrap_servers: list[str] = field(default_factory=lambda: ["localhost:9092"])
    topic_transactions: str = "transactions"
    topic_fraud_alerts: str = "fraud-alerts"
    group_id: str = "fraud-detection-group"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    
    @property
    def bootstrap_servers_str(self) -> str:
        """Retorna bootstrap servers como string."""
        return ",".join(self.bootstrap_servers)


KAFKA_CONFIG = KafkaConfig()
