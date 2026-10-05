"""Configurações do Postgres (lê .env / ambiente, com defaults do compose)."""

import os
from dataclasses import dataclass, field


@dataclass
class PostgresConfig:
    """Conexão Postgres. Prefere POSTGRES_DSN; senão monta das partes."""

    dsn: str = field(
        default_factory=lambda: os.getenv(
            "POSTGRES_DSN",
            f"postgresql://{os.getenv('POSTGRES_USER', 'fraud')}"
            f":{os.getenv('POSTGRES_PASSWORD', 'fraudpass_change_me')}"
            f"@{os.getenv('POSTGRES_HOST', 'localhost')}"
            f":{os.getenv('POSTGRES_PORT', '5432')}"
            f"/{os.getenv('POSTGRES_DB', 'frauddb')}",
        )
    )


POSTGRES_CONFIG = PostgresConfig()
