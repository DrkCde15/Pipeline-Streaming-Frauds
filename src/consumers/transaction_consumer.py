"""Consumer de transações do Kafka."""

import json
import logging
from typing import Any, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.kafka_config import KAFKA_CONFIG

from src.schemas.transaction import Transacao

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """Consumer de transações com processamento básico."""
    
    def __init__(self) -> None:
        """Inicializa o consumer Kafka."""
        self.consumer = KafkaConsumer(
            KAFKA_CONFIG.topic_transactions,
            bootstrap_servers=KAFKA_CONFIG.bootstrap_servers_str,
            group_id=KAFKA_CONFIG.group_id,
            auto_offset_reset=KAFKA_CONFIG.auto_offset_reset,
            enable_auto_commit=KAFKA_CONFIG.enable_auto_commit,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            consumer_timeout_ms=10000,
        )
        self.transacoes_processadas: list[dict[str, Any]] = []
        self.transacoes_fraudulentas: list[dict[str, Any]] = []
    
    def processar_transacao(self, transacao: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Processa uma transação individual.
        
        Args:
            transacao: Dados da transação
            
        Returns:
            Transação processada ou None se inválida
        """
        # Validação via schema unificado (tolera sintético + creditcard_csv)
        try:
            tx = Transacao.from_dict(transacao)
        except (KeyError, TypeError, ValueError) as e:
            logger.warning(f"Transação inválida p/ schema: {e}")
            return None
        erros = tx.validar()
        if erros:
            logger.warning(f"Transação {tx.transaction_id} inválida: {erros}")
            return None

        # Processamento básico (preserva V1..V28/source p/ ML v2 e Postgres)
        d = tx.to_dict()
        transacao_processada = {
            "transaction_id": d["transaction_id"],
            "user_id": d["user_id"],
            "valor": float(d["valor"]),
            "timestamp": d["timestamp"],
            "localizacao": d.get("localizacao", {}),
            "dispositivo": d.get("dispositivo", "unknown"),
            "categoria": d.get("categoria", "outros"),
            "is_fraud": d.get("is_fraud", False),
            "source": d.get("source", "synthetic"),
            "time_original": d.get("time_original"),
            "v_features": d.get("v_features"),
            "processed_at": __import__("datetime").datetime.utcnow().isoformat(),
        }
        # Mantém V1..V28 achatadas se existirem (compat Postgres)
        for i in range(1, 29):
            if f"V{i}" in d:
                transacao_processada[f"V{i}"] = d[f"V{i}"]

        return transacao_processada
    
    def executar(self, max_transacoes: Optional[int] = None) -> None:
        """Executa o consumer.
        
        Args:
            max_transacoes: Limite máximo de transações (None = infinito)
        """
        logger.info("🚀 Iniciando Consumer de Transações...")
        logger.info(f"📡 Kafka: {KAFKA_CONFIG.bootstrap_servers_str}")
        logger.info(f"📋 Tópico: {KAFKA_CONFIG.topic_transactions}")
        logger.info(f"👥 Group ID: {KAFKA_CONFIG.group_id}")
        
        contador = 0
        
        try:
            for mensagem in self.consumer:
                transacao = mensagem.value
                
                if transacao is None:
                    continue
                
                # Processa a transação
                transacao_processada = self.processar_transacao(transacao)
                
                if transacao_processada:
                    self.transacoes_processadas.append(transacao_processada)
                    
                    # Verifica se é fraudulenta
                    if transacao_processada["is_fraud"]:
                        self.transacoes_fraudulentas.append(transacao_processada)
                        logger.warning(
                            f"🚨 FRAUDE DETECTADA | ID: {transacao_processada['transaction_id'][:8]} | "
                            f"Valor: R$ {transacao_processada['valor']:.2f}"
                        )
                    else:
                        logger.info(
                            f"✅ Transação OK | ID: {transacao_processada['transaction_id'][:8]} | "
                            f"Valor: R$ {transacao_processada['valor']:.2f}"
                        )
                
                contador += 1
                
                if max_transacoes and contador >= max_transacoes:
                    break
        
        except KeyboardInterrupt:
            logger.info("⏹️  Consumer interrompido pelo usuário")
        except KafkaError as e:
            logger.error(f"Erro Kafka: {e}")
        finally:
            self._finalizar()
    
    def _finalizar(self) -> None:
        """Finaliza o consumer e imprime estatísticas."""
        total = len(self.transacoes_processadas)
        fraudes = len(self.transacoes_fraudulentas)
        taxa_fraude = (fraudes / total * 100) if total > 0 else 0
        
        logger.info("=" * 50)
        logger.info("📊 ESTATÍSTICAS FINAIS")
        logger.info(f"   Total processadas: {total}")
        logger.info(f"   Fraudulentas: {fraudes}")
        logger.info(f"   Taxa de fraude: {taxa_fraude:.2f}%")
        logger.info("=" * 50)
        
        self.consumer.close()
        logger.info("✅ Consumer finalizado.")
    
    def obter_fraudulentas(self) -> list[dict[str, Any]]:
        """Retorna lista de transações fraudulentas detectadas."""
        return self.transacoes_fraudulentas.copy()


def main() -> None:
    """Função principal do consumer."""
    consumer = TransactionConsumer()
    consumer.executar(max_transacoes=1000)


if __name__ == "__main__":
    main()
