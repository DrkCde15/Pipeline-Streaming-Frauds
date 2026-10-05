# Revisão de Engenharia de Dados — streaming-fraudes

**Data:** 05/10/2026
**Escopo:** repositório local (código, SQL, compose, notebook, dashboard, git). Sem acesso a produção — e nem há produção; análise estática + comandos só-leitura.
**Maturidade assumida:** Estágio 1 (estudo/portfólio, time de 1, sem SLA/consumidor externo) — o README vende aprendizado e demo, não serviço.

## Veredito
É um bom projeto de Estágio 1: pipeline real Kafka→Postgres→Grafana funcionando, schema unificado testado (16 testes verdes) e documentação honesta. Mas há um problema **crítico de idempotência** (re-replay duplica dados, violando um inegociável de qualquer estágio), o **ML do headline não participa do serving** e o **commit atual está defasado** — quase todo o valor construído (writer, testes, dashboard Postgres) ainda não foi commitado. Os 3 itens do "Agora" custam horas e destravam o push com segurança.

## Mapa do ciclo de vida
| Etapa do ciclo | Onde está no repo | Tecnologia | Observação |
|---|---|---|---|
| Geração (fontes) | `data/raw/creditcard.csv` (gitignored) + `src/producers/transaction_producer.py` | ULB/Worldline + gerador sintético | Fonte documentada no README; sem contrato (dataset público estático) |
| Armazenamento | `db/schema.sql`, `docker-compose.yml` (postgres:16) | PostgreSQL | Só camada servida; **sem bruto imutável** |
| Ingestão | `src/producers/` + `src/consumers/transaction_consumer.py` | Kafka KRaft (tópicos `transactions`, `fraud-alerts`) | Replay + validação + commit manual; sem retry/DLQ |
| Transformação | `src/detectors/rule_engine.py`, `src/detectors/ml_detector.py`, `src/schemas/` | Python + RandomForest | Regras no stream; **ML só manual** |
| Disponibilização | `grafana/dashboard.json` v2, `notebooks/01_analise_fraudes.ipynb` | Grafana→Postgres, Jupyter | Dashboard funciona; sem frescor visível |

## Scorecard
| Dimensão | Nota (0–3) | Esperado no estágio | Resumo em uma linha |
|---|---|---|---|
| Geração | 2 | 1–2 | Fonte real + sintética, documentadas |
| Armazenamento | 1 | 1 | Schema bom, mas sem camada raw |
| Ingestão | 2 | 1–2 | Idempotente no reprocessamento, replay não |
| Transformação | 1 | 1 | Regras testadas; ML fora do path |
| Disponibilização | 1 | 1 | Dashboard ok; sem frescor, ML ausente |
| Segurança | 2 | 1–2 | Sem segredos no git; credenciais só placeholders locais |
| Gerenciamento | 1 | 1 | README/dicionário parciais; sem testes de dados |
| DataOps | 1 | 0–1 | Testes unitários, sem CI/monitoramento |
| Arquitetura | 2 | 1–2 | Compose + KRaft adequado; over-eng. removido (pyspark) |
| Orquestração | 1 | 0–1 | Execução manual; Airflow recusado com acerto |
| Eng. software | 2 | 1–2 | Modular, tipado, testado; sem lockfile/lint |

## Pontos fortes verificados
- `src/storage/postgres_writer.py`: `ON CONFLICT DO NOTHING` — reprocessamento da **mesma** mensagem Kafka não duplica.
- `src/consumers/transaction_consumer.py`: commit manual após processar — restart avança no log (bug anterior corrigido).
- `tests/` com 16 testes que rodam sem Kafka/DB/CSV (inclui regressão do `NameError` geográfico).
- `.gitignore` com `.env`, `data/`, `.venv/`; `.env` local contém só placeholders de dev; CSV de 144MB fora do git; notebook sem outputs commitados.

## Achados
### R1 Re-replay do CSV duplica linhas lógicas — Crítica · Esforço P
- **Local:** `src/schemas/transaction.py:143` (`transaction_id=str(uuid.uuid4())`)
- **Status:** verificado
- **Evidência:** cada `from_creditcard_row` gera UUID novo; `ON CONFLICT` só protege reprocessamento da mesma mensagem, não um segundo `enviar_csv` (linhas gêmeas com PKs diferentes, mesmo `Time`/`Amount`/`V`s).
- **Por que importa:** viola o inegociável "reprocessar não duplica" — o erro silencioso clássico: counts e taxa de fraude inflados sem alarme.
- **Como corrigir:** ID determinístico no replay (ex. `uuid5` de `Time|Amount|V1|V2|V3`) ou `UNIQUE(Time, Amount, V1, V2)` + `ON CONFLICT DO NOTHING`.

### R2 ML não participa do serving — Alta · Esforço M
- **Local:** `src/consumers/` (ausência: nenhum `prever`/`MLDetector`); `src/detectors/ml_detector.py:182` (`salvar_modelo` definido, zero usos)
- **Status:** verificado
- **Evidência:** `grep` por `prever|MLDetector|treinar` em consumers/producers retorna vazio; alertas do stream vêm só de regras + label do CSV.
- **Por que importa:** o projeto anuncia "regras + ML em tempo real"; quem consome o dashboard recebe score só de regras (F1 ~1% no dado real vs 85% do modelo).
- **Como corrigir:** carregar modelo v2 treinado no boot do consumer (`treinar_com_csv` uma vez ou artefato em disco via `salvar_modelo`), chamar `prever_v2` por mensagem com fallback para regras se o modelo falhar.

### R3 Timestamps naive × aware quebram regras no stream misto — Alta · Esforço P
- **Local:** `src/producers/transaction_producer.py:53,75` (`datetime.utcnow()`, naive) vs `src/schemas/transaction.py` (`BASE_TIME_ULB` aware UTC); `src/detectors/rule_engine.py` compara com `fromisoformat`
- **Status:** verificado no código; disparo em produção inferido (stream misto não executado)
- **Evidência:** `user_id` usa o mesmo range 1–1000 nas duas fontes; comparar naive com aware levanta `TypeError`, engolido pelo `try/except` de `_checar_regras` → regras puladas com só um warning no log.
- **Por que importa:** fuso implícito + erro engolido = alertas suprimidos silenciosamente.
- **Como corrigir:** normalizar tudo para aware UTC na fronteira (`Transacao.validar`/`from_dict` converte naive → UTC) e um teste misto.

### R4 `requirements-dev.txt` deletado do disco — Alta · Esforço P
- **Local:** `requirements-dev.txt` (tracked no HEAD, `git status` acusa `D`, arquivo ausente no disco)
- **Status:** verificado
- **Evidência:** `ls requirements*.txt` só mostra `requirements.txt`; era o único lugar que declarava `pytest/matplotlib/seaborn/jupyter`.
- **Por que importa:** quebra o inegociável "outra pessoa roda seguindo o README" (notebook e testes não instaláveis).
- **Como corrigir:** restaurar o arquivo (conteúdo conhecido: pytest/matplotlib/seaborn/jupyter) e commitar.

### R5 Quase todo o valor está fora do commit — Média · Esforço P
- **Local:** `git status`: `M README.md, config/kafka_config.py, grafana/dashboard.json, notebook, requirements.txt, consumer` + `?? .env.example, config/db_config.py, docker-compose.yml, src/storage/, 3 arquivos de teste`
- **Status:** verificado
- **Evidência:** o HEAD (`first commit`) contém o dashboard Prometheus antigo e o consumer sem writer/commit.
- **Por que importa:** perda de trabalho a um `rm` de distância + remoto defasado conta história errada do projeto.
- **Como corrigir:** `git add -A && git commit` (`.env` e `data/` já ignorados — conferido) + push.

### R6 Sem camada de bruto imutável — Média · Esforço M
- **Local:** `db/schema.sql` (só tabelas servidas); `data/` gitignored sem versionamento
- **Status:** verificado
- **Evidência:** o Postgres guarda a transação já convertida; o CSV original não existe em nenhuma tabela raw; combinado com R1, o passado não é reconstruível bit-a-bit.
- **Por que importa:** sem bruto, cada correção de schema/conversão exige o CSV local de alguém.
- **Como corrigir (estágio 2):** tabela `raw_creditcard` (linha fiel do CSV) ou documentar o CSV como bronze imutável + fix R1.

### R7 `dispositivo_novo` dispara na 1ª transação de todo usuário — Média · Esforço P
- **Local:** `src/detectors/rule_engine.py:155-170`
- **Status:** verificado (e há teste que consagra o comportamento)
- **Evidência:** check-then-register: primeiro dispositivo sempre desconhecido → 100% dos usuários geram 1 alerta.
- **Por que importa:** infla `fraud_alerts` e o painel "Alertas por Regra" com ruído sistemático.
- **Como corrigir:** exigir 2º sinal junto (ex. `dispositivo_novo` só alerta com `horario_incomum` ou score ≥ limiar) ou semear dispositivos conhecidos.

### R8 Dependências sem lock (`>=` soltos) — Média · Esforço P
- **Local:** `requirements.txt` (`kafka-python>=2.0.2` resolveu `3.0.11`)
- **Status:** verificado
- **Evidência:** dry-run instalou majors novos (pandas 3, sklearn 1.9) vs pins originais de 2023.
- **Por que importa:** "funciona na minha máquina" com data marcada; quebra futura sem mudança de código.
- **Como corrigir:** `pip freeze > requirements.lock` (ou `pip-tools`) e CI instalando do lock.

### R9 Split temporal 2013 × 2026 sem convenção — Média · Esforço P
- **Local:** `src/schemas/transaction.py` (base 2013) vs `transaction_producer.py:53` (`utcnow`)
- **Status:** verificado (foi a causa do "No Data" no Grafana)
- **Evidência:** dashboard default 2013 esconde o sintético; janela "última 1h" esconde o CSV.
- **Por que importa:** consumidor do dashboard tira conclusão errada ("sem fraude") por filtro, não por dado.
- **Como corrigir:** documentar no dashboard/README ou time-shift no `enviar_csv`.

### R10 Sem frescor visível no dashboard — Baixa · Esforço P
- **Local:** `grafana/dashboard.json` (ausência de painel `MAX(timestamp)/MAX(processed_at)`)
- **Status:** verificado
- **Evidência:** nenhum painel responde "os dados vão até quando?".
- **Como corrigir:** stat `SELECT MAX(timestamp) FROM transactions`.

### R11 Consumer "streaming" sai após 10s ocioso — Baixa · Esforço P
- **Local:** `src/consumers/transaction_consumer.py:35` (`consumer_timeout_ms=10000`)
- **Status:** verificado
- **Evidência:** `for mensagem in self.consumer` termina em silêncio; operador espera daemon.
- **Como corrigir:** documentar no README ou remover o timeout no modo serviço.

### R12 Producer síncrono (1 round-trip por mensagem) — Baixa · Esforço P
- **Local:** `src/producers/transaction_producer.py:123-128,166-171` (`future.get` por envio)
- **Status:** verificado
- **Evidência:** 2000 mensagens ≈ 2000 RTTs; adequado para demo, insuficiente para carga.
- **Como corrigir:** só se precisar — `linger_ms` + batch assíncrono com callback de erro (não fazer agora).

## Roadmap
1. **Agora** — R1 (ID determinístico no replay), R3 (UTC aware na fronteira + teste misto), R4 (restaurar dev-requirements), R5 (commit + push), documentar R11. Tudo P, tudo antes do push.
2. **Em seguida** — R2 (ML no serving com fallback), R6 (tabela raw), R8 (lockfile + CI mínimo `pytest`), R10 (frescor), R7 (regra de dispositivo), R9 (decidir time-shift).
3. **Deliberadamente adiado** — Airflow/scheduler (sem jobs recorrentes ainda), Spark/cluster (dados cabem em memória), exporter Prometheus (Postgres direto resolve no estágio 1), catálogo/linhagem formal, multi-ambiente dev/prod.

## O que esta análise não cobriu
Produção e volumes reais (não há), custos e FinOps (tudo local/Podman), permissões em nuvem (n/a), qualidade real dos dados além do ULB público, comportamento sob carga/concorrência, e o que vive fora do repo (conta Kaggle, datasource Grafana criado via UI, `.env` local). Segredos: verifiquei `.env`/`.env.example` — só placeholders de dev, nada real exposto.

## Perguntas em aberto
1. O destino é portfólio/estudo ou há plano de produção? (muda o scorecard de DataOps/orquestração)
2. No replay, prefere time-shift para "agora" ou manter as datas de 2013? (decide R9)
3. `requirements-dev.txt` foi deletado de propósito ou por acidente?
4. Quer ML no consumer já (R2) ou o scope atual "regras no stream + ML offline" está bom para a v0.1?
