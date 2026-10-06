"""Consumer de transações do Kafka."""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from config.kafka_config import KAFKA_CONFIG

from src.schemas.transaction import Transacao

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """Consumer de transações com validação, regras, ML, Postgres e fraud-alerts."""

    def __init__(
        self,
        persist: bool = True,
        ml: bool = True,
        model_path: str = "models/fraud_rf_v2.pkl",
    ) -> None:
        """Inicializa o consumer Kafka.

        Args:
            persist: salva em Postgres e publica fraud-alerts (desliga p/ demo sem DB)
            ml: tenta ML v2 no serving (fallback p/ regras se indisponível)
            model_path: artefato gerado por scripts/train_ml.py
        """
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
        self.persist = persist
        self._pg = None
        self._alert_producer = None
        self._persistidas = 0
        self._alertas = 0
        # RuleEngine com estado (histórico por user p/ velocidade/geográfico)
        from src.detectors.rule_engine import RuleEngine

        self.rules = RuleEngine()
        self._ml = self._init_ml(model_path) if ml else None
    
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
            "processed_at": datetime.now(timezone.utc).isoformat(),
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

                    # 1) Persiste no Postgres (idempotente, nunca quebra o stream)
                    if self.persist:
                        self._salvar_no_banco(transacao_processada)

                    # 2) Regras + ML v2 -> alerta em fraud-alerts + fraud_alerts
                    # ML ausente/falho => fallback automático p/ regras (R2)
                    from src.detectors.rule_engine import decidir_alerta

                    alerta_rules = self._checar_regras(transacao_processada)
                    ml_prob = self._checar_ml(transacao_processada)
                    regras = (
                        list(alerta_rules.regras_ativadas) if alerta_rules else []
                    )
                    score_rules = float(alerta_rules.score) if alerta_rules else 0.0
                    dispara, score = decidir_alerta(regras, score_rules, ml_prob)
                    if dispara and self.persist:
                        self._publicar_alerta(
                            transacao_processada, regras, score, ml_prob
                        )

                    # Label original (ground truth do CSV) só p/ estatística
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

                # Commit manual: sem isso (auto_commit=False) cada restart
                # relê do início e nunca avança no log.
                try:
                    self.consumer.commit()
                except Exception:
                    pass

                if max_transacoes and contador >= max_transacoes:
                    break
        
        except KeyboardInterrupt:
            logger.info("⏹️  Consumer interrompido pelo usuário")
        except KafkaError as e:
            logger.error(f"Erro Kafka: {e}")
        finally:
            self._finalizar()
    
    def _pg_conn(self):
        """Conexão lazy (só conecta se persist=True e DB acessível)."""
        if self._pg is None:
            from src.storage.postgres_writer import get_conn

            self._pg = get_conn()
        return self._pg

    def _salvar_no_banco(self, tx: dict[str, Any]) -> None:
        try:
            from src.storage.postgres_writer import salvar_transacao

            if salvar_transacao(self._pg_conn(), tx):
                self._persistidas += 1
        except Exception as e:
            logger.warning(f"Postgres indisponível, seguindo sem persistir: {e}")
            self._pg = None

    def _checar_regras(self, tx: dict[str, Any]):
        try:
            return self.rules.verificar_transacao(tx)
        except Exception as e:
            logger.warning(f"RuleEngine falhou p/ {tx['transaction_id'][:8]}: {e}")
            return None

    def _init_ml(self, model_path: str):
        """Carrega ML v2 p/ o serving (R2). Ordem: artefato > treino > regras.

        Sem artefato e com CSV presente, treina e salva (boot lento, uma vez).
        Sem nenhum dos dois, retorna None e o serving segue só com regras.
        """
        import os
        from pathlib import Path

        model_path = os.getenv("MODEL_PATH", model_path)
        csv_path = os.getenv("CREDITCARD_CSV", "data/raw/creditcard.csv")
        try:
            from src.detectors.ml_detector import MLDetector

            det = MLDetector()
            if Path(model_path).exists():
                det._carregar_modelo(model_path)
                logger.info(f"ML v2 carregado de {model_path}")
            elif Path(csv_path).exists():
                logger.warning("Artefato ML ausente; treinando v2 no boot (lento, uma vez)...")
                det.treinar_com_csv(csv_path)
                det.salvar_modelo(model_path)
            else:
                logger.warning("Sem modelo nem CSV: ML desabilitado, só regras.")
                return None
            if det.modelo_v2 is None:
                return None
            return det
        except Exception as e:
            logger.warning(f"ML indisponível, seguindo só com regras: {e}")
            return None

    def _checar_ml(self, tx: dict[str, Any]) -> Optional[float]:
        """Retorna P(fraude) do ML ou None. Falha => desabilita p/ o resto do run."""
        if self._ml is None:
            return None
        try:
            return float(self._ml.prever_v2(tx)["probabilidade_fraude"])
        except Exception as e:
            logger.warning(f"ML falhou, desabilitado neste run (fallback regras): {e}")
            self._ml = None
            return None

    def _publicar_alerta(
        self,
        tx: dict[str, Any],
        regras: list[str],
        score: float,
        ml_prob: Optional[float] = None,
    ) -> None:
        try:
            import json as _json

            from kafka import KafkaProducer

            from src.storage.postgres_writer import salvar_alerta

            salvar_alerta(
                self._pg_conn(),
                tx["transaction_id"],
                list(regras),
                float(score),
                ml_prob,
            )
            if self._alert_producer is None:
                self._alert_producer = KafkaProducer(
                    bootstrap_servers=KAFKA_CONFIG.bootstrap_servers_str,
                    value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
                )
            self._alert_producer.send(
                KAFKA_CONFIG.topic_fraud_alerts,
                value={
                    "transaction_id": tx["transaction_id"],
                    "regras": list(regras),
                    "score": float(score),
                    "ml_probabilidade": ml_prob,
                    "valor": float(tx["valor"]),
                },
            )
            self._alertas += 1
        except Exception as e:
            logger.warning(f"Alerta não persistido/publicado: {e}")
            self._pg = None

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
        if self.persist:
            logger.info(f"   Persistidas Postgres: {self._persistidas}")
            logger.info(f"   Alertas (regras+ML): {self._alertas}")
            logger.info(f"   ML v2: {'ativo' if self._ml is not None else 'inativo (só regras)'}")
        logger.info("=" * 50)

        try:
            if self._alert_producer is not None:
                self._alert_producer.flush()
                self._alert_producer.close()
        except Exception:
            pass
        try:
            if self._pg is not None:
                self._pg.close()
        except Exception:
            pass
        
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
