# Streaming + Fraud Detection

Sistema de detecção de fraudes em transações financeiras com Apache Kafka, regras de domínio e Machine Learning. v0.1 usa o dataset real ULB/Worldline (creditcard.csv) com schema unificado para streaming, treino e Postgres.

## Visão Geral

Pipeline:

```
[creditcard.csv / gerador sintético]
  -> [Transaction Producer] -> [Kafka: transactions]
  -> [Transaction Consumer (valida schema)]
  -> [RuleEngine + MLDetector v2] -> [PostgreSQL] -> [Grafana*]
```

`*` Dashboard em `grafana/dashboard.json` espera métricas Prometheus (`transactions_total`, `fraud_alerts_total`). Exporter ainda não implementado na v0.1.

## Schema Unificado

Centralizado em `src/schemas/transaction.py` (`Transacao`):

- Negócio (sempre presentes): `transaction_id, user_id, valor (=Amount), timestamp (ISO8601 UTC), moeda, localizacao {cidade, pais, latitude, longitude}, dispositivo, categoria, is_fraud (=Class)`
- ML real (quando `source == 'creditcard_csv'`): `time_original (=Time), v_features (=V1..V28, 28 floats)`
- `source`: `synthetic` | `creditcard_csv`
- `to_dict()` achata `V1..V28` para Kafka/Postgres. `from_dict()` / `from_creditcard_row()` / `validar()` / `to_ml_vector_v2()` ([Time + V1..V28 + Amount] = 30 dims).

Limitação conhecida: o CSV é anonimizado via PCA, sem geo/dispositivo/categoria reais. Esses campos são preenchidos de forma determinística só para compatibilidade com RuleEngine/dashboard. O modelo ML v2 não usa esses campos.

Banco em `db/schema.sql`: tabelas `transactions` (com `V1..V28, time_original, source`) e `fraud_alerts`. Aplicar com:

```bash
psql $DATABASE_URL -f db/schema.sql
```

## Dataset Real

Dataset ULB/Worldline - European cardholders, setembro/2013:

- 284.807 transações, 492 fraudes (0,17%)
- Colunas: `Time, V1..V28, Amount, Class`
- Arquivo local esperado: `data/raw/creditcard.csv` (ignorado pelo git, 144 MB)

Baixar:

```bash
pip install kaggle
# configure ~/.kaggle/kaggle.json (https://www.kaggle.com/settings -> API -> Create New Token)
kaggle datasets download mlg-ulb/creditcardfraud -p data/raw --unzip
ls -lh data/raw/creditcard.csv
```

Fonte: https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud

## Instalação

Requer Python 3.11+ e Kafka acessível (padrão `localhost:9092`).

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Config Kafka em `config/kafka_config.py` (`bootstrap_servers, topic_transactions=transactions, topic_fraud_alerts=fraud-alerts, group_id`). Sem `.env` na v0.1.

## Uso

### 1. Replay do CSV real para o Kafka

```python
from src.producers.transaction_producer import criar_producer, enviar_csv

producer = criar_producer()
try:
    enviar_csv(producer, csv_path="data/raw/creditcard.csv", max_linhas=1000)
finally:
    producer.close()
```

Ou geração sintética (demo sem CSV):

```bash
python -m src.producers.transaction_producer
```

### 2. Consumir e validar schema

```bash
python -m src.consumers.transaction_consumer
```

Nota (R11): o consumer sai sozinho após 10s sem mensagens (`consumer_timeout_ms=10000`).
Para drenar o log inteiro, repita a chamada ou use `TransactionConsumer().executar(max_transacoes=5000)`.
Offsets têm commit manual, então cada run avança de onde parou.

Validação isolada (sem Kafka):

```python
import csv
from src.schemas.transaction import Transacao

with open("data/raw/creditcard.csv") as f:
    row = next(csv.DictReader(f))

tx = Transacao.from_creditcard_row(row, idx=0)
print(tx.validar())  # [] = válida
print(tx.to_ml_vector_v2()[:3])
```

### 3. Regras + ML

```python
from src.detectors.rule_engine import RuleEngine
from src.detectors.ml_detector import MLDetector

engine = RuleEngine()
alerta = engine.verificar_transacao(tx.to_dict())

ml = MLDetector()
metricas = ml.treinar_com_csv("data/raw/creditcard.csv")
print(metricas)  # precision/recall/f1 no holdout 80/20
print(ml.prever_v2(tx.to_dict()))
```

Regras (ver `src/detectors/rule_engine.py`):

- `velocidade` (peso 2.0): 5+ txs do mesmo user em 1 min
- `valor_alto` (peso 1.5): valor > R$ 10.000
- `geografico` (peso 2.5): países diferentes em 5 min
- `horario_incomum` (peso 1.0): 00h-05h
- `dispositivo_novo` (peso 1.2): primeiro uso do dispositivo

Modelo v1 (legado): RandomForest 8 features sintéticas. Modelo v2 (atual): RandomForest 200 árvores, `class_weight=balanced`, 30 features reais.

### ML no serving (R2)

```bash
python scripts/train_ml.py   # gera models/fraud_rf_v2.pkl (gitignored, 8MB)
```

O consumer carrega o artefato no boot; sem ele, treina sozinho (lento, uma vez) ou segue só com regras. Por mensagem, dispara alerta se **regras OU `P(fraude) >= 0.5`**, e grava `ml_probabilidade` em `fraud_alerts`. Falha no ML nunca derruba o stream (fallback automático). Env: `MODEL_PATH`, `CREDITCARD_CSV`.

### Tópicos Kafka

- `transactions` - transações no schema unificado
- `fraud-alerts` - reservado (producer de alertas ainda não implementado na v0.1)

## Estrutura

```
config/kafka_config.py       # lê KAFKA_* do .env
config/db_config.py          # POSTGRES_DSN
src/schemas/transaction.py   # schema canônico + IDs determinísticos
src/producers/               # sintético + enviar_csv()
src/consumers/               # validação + regras + ML v2 + Postgres + fraud-alerts
src/detectors/rule_engine.py # 5 regras + decidir_alerta()
src/detectors/ml_detector.py # v1 sintético + v2 real (salvar/carregar)
src/storage/                 # writer Postgres idempotente
scripts/train_ml.py          # gera models/fraud_rf_v2.pkl
db/schema.sql
data/raw/                    # creditcard.csv (gitignored)
grafana/dashboard.json       # SQL direto no Postgres
notebooks/01_analise_fraudes.ipynb  # EDA no CSV real
tests/                       # 26 testes (pytest -q)
```

## Testes

```bash
pytest tests/ -q
```

Status: 26 testes (`pytest tests/ -q`), incluindo integração Postgres (pula sozinho sem DB).

## Roadmap v0.2

- [x] Testes: `Transacao.from_creditcard_row`, roundtrip Kafka, regras (`pytest tests/ -q` → 16 verdes)
- [x] `.env.example` + `docker-compose` (Kafka, Postgres, Grafana)
- [x] Writer Postgres no consumer + producer `fraud-alerts`
- [x] Exporter Prometheus ou ajuste do dashboard para Postgres
- [x] Migrar notebook para o CSV real
- [x] R2: ML v2 no serving com fallback p/ regras (`scripts/train_ml.py` + `decidir_alerta`)

## Licença

MIT
