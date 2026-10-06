"""Producer de transações para Kafka."""

import json
import random
import uuid
from datetime import UTC, datetime
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.kafka_config import KAFKA_CONFIG
from src.schemas.transaction import Transacao

# Cidades e países simulados
CIDADES = [
    {"cidade": "São Paulo", "pais": "BR", "lat": -23.55, "lon": -46.63},
    {"cidade": "Rio de Janeiro", "pais": "BR", "lat": -22.91, "lon": -43.17},
    {"cidade": "Belo Horizonte", "pais": "BR", "lat": -19.92, "lon": -43.94},
    {"cidade": "Salvador", "pais": "BR", "lat": -12.97, "lon": -38.51},
    {"cidade": "Curitiba", "pais": "BR", "lat": -25.43, "lon": -49.27},
    {"cidade": "Porto Alegre", "pais": "BR", "lat": -30.03, "lon": -51.23},
    {"cidade": "Brasília", "pais": "BR", "lat": -15.78, "lon": -47.93},
    {"cidade": "Fortaleza", "pais": "BR", "lat": -3.72, "lon": -38.54},
    {"cidade": "Manaus", "pais": "BR", "lat": -3.12, "lon": -60.02},
    {"cidade": "Belém", "pais": "BR", "lat": -1.46, "lon": -48.50},
    {"cidade": "Nova York", "pais": "US", "lat": 40.71, "lon": -74.01},
    {"cidade": "Londres", "pais": "GB", "lat": 51.51, "lon": -0.13},
    {"cidade": "Tóquio", "pais": "JP", "lat": 35.68, "lon": 139.69},
    {"cidade": "Paris", "pais": "FR", "lat": 48.86, "lon": 2.35},
    {"cidade": "Buenos Aires", "pais": "AR", "lat": -34.60, "lon": -58.38},
]

DISPOSITIVOS = ["mobile", "desktop", "tablet", "pos", "atm"]

CATEGORIAS = [
    "alimentacao",
    "transporte",
    "saude",
    "educacao",
    "lazer",
    "compras",
    "servicos",
    "transferencia",
    "saque",
    "deposito",
]


def gerar_transacao_fraudulenta() -> dict[str, Any]:
    """Gera uma transação com características fraudulentas."""
    local = random.choice(CIDADES[:10])  # Apenas Brasil para fraudes

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "valor": round(random.uniform(5000, 50000), 2),
        "moeda": "BRL",
        "timestamp": datetime.now(UTC).isoformat(),
        "localizacao": {
            "cidade": local["cidade"],
            "pais": local["pais"],
            "latitude": local["lat"],
            "longitude": local["lon"],
        },
        "dispositivo": random.choice(DISPOSITIVOS),
        "categoria": random.choice(CATEGORIAS),
        "is_fraud": True,
    }


def gerar_transacao_normal() -> dict[str, Any]:
    """Gera uma transação legítima."""
    local = random.choice(CIDADES)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "valor": round(random.uniform(10, 2000), 2),
        "moeda": "BRL",
        "timestamp": datetime.now(UTC).isoformat(),
        "localizacao": {
            "cidade": local["cidade"],
            "pais": local["pais"],
            "latitude": local["lat"],
            "longitude": local["lon"],
        },
        "dispositivo": random.choice(DISPOSITIVOS),
        "categoria": random.choice(CATEGORIAS),
        "is_fraud": False,
    }


def criar_producer() -> KafkaProducer:
    """Cria e retorna um KafkaProducer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_CONFIG.bootstrap_servers_str,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def enviar_transacoes(
    producer: KafkaProducer,
    num_transacoes: int = 100,
    taxa_fraude: float = 0.1,
) -> None:
    """Envia transações para o tópico Kafka.

    Args:
        producer: Produtor Kafka
        num_transacoes: Número total de transações
        taxa_fraude: Proporção de transações fraudulentas (0-1)
    """
    topic = KAFKA_CONFIG.topic_transactions

    for i in range(num_transacoes):
        # Decide se é fraude
        is_fraud = random.random() < taxa_fraude

        if is_fraud:
            transacao = gerar_transacao_fraudulenta()
        else:
            transacao = gerar_transacao_normal()

        try:
            future = producer.send(
                topic=topic,
                key=str(transacao["user_id"]),
                value=transacao,
            )
            record_metadata = future.get(timeout=10)

            status = "FRAUDE" if is_fraud else "Normal"
            print(
                f"[{i + 1}/{num_transacoes}] {status} | "
                f"ID: {transacao['transaction_id'][:8]} | "
                f"Valor: R$ {transacao['valor']:.2f} | "
                f"Topic: {record_metadata.topic}"
            )

        except KafkaError as e:
            print(f"Erro ao enviar transação: {e}")

    producer.flush()
    print(f"\nTotal de transações enviadas: {num_transacoes}")


def enviar_csv(
    producer: KafkaProducer,
    csv_path: str = "data/raw/creditcard.csv",
    max_linhas: int | None = 1000,
) -> None:
    """Replay do creditcard.csv real para o tópico Kafka.

    Converte cada linha via Transacao.from_creditcard_row (schema unificado).
    Mantém o label real (Class -> is_fraud) para avaliação.
    """
    import csv

    topic = KAFKA_CONFIG.topic_transactions

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if max_linhas is not None and i >= max_linhas:
                break
            tx = Transacao.from_creditcard_row(row, idx=i)
            try:
                future = producer.send(
                    topic=topic,
                    key=str(tx.user_id),
                    value=tx.to_dict(),
                )
                future.get(timeout=10)
                if (i + 1) % 200 == 0:
                    print(f"[{i + 1}] replay CSV... último valor R$ {tx.valor:.2f}")
            except KafkaError as e:
                print(f"Erro ao enviar linha {i}: {e}")

    producer.flush()
    print(f"\nReplay CSV finalizado: {csv_path}")


def main() -> None:
    """Função principal do producer."""
    print("🚀 Iniciando Producer de Transações...")
    print(f"📡 Kafka: {KAFKA_CONFIG.bootstrap_servers_str}")
    print(f"📋 Tópico: {KAFKA_CONFIG.topic_transactions}")

    producer = criar_producer()

    try:
        enviar_transacoes(
            producer=producer,
            num_transacoes=1000,
            taxa_fraude=0.15,
        )
    finally:
        producer.close()
        print("✅ Producer finalizado.")


if __name__ == "__main__":
    main()
