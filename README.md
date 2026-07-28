# Media Pipeline 2026

Pipeline analítico end-to-end para harmonização e modelagem semântica de campanhas multicanal de mídia paga. Construído do zero no GCP.

## O problema

5 plataformas de mídia paga. 5 schemas incompatíveis. Nenhum padrão entre elas.

O que Meta chama de `cliques_link`, Google chama de `clicks`. O que DV360 chama de `investimento`, Google Ads chama de `custo`. DV360 nao tem cliques: e comprado por CPM. VTR não existe em Search.

Além disso: a mesma campanha aparece como `pmax_awareness_cacaushow_2026` no Google Ads e `pmx_awareness_cacaushow` em outro sistema . O mesmo nome com um typo, tratado como duas campanhas diferentes.

Sem resolver esses problemas, CTR médio "cross-canal" é mentira.

## A solução: Medallion Architecture

```
Fontes (5 plataformas de mídia paga)
Google PMAX · Google Ads · Google Shopping · Meta Ads · DV360
        ↓
   Streamlit + OAuth 2.0 PKCE
   Upload autenticado → BigQuery RAW
        ↓
┌─────────────────────────────┐
│       BigQuery RAW          │
│  fiel à origem, sem toque   │
└─────────────────────────────┘
        ↓  PySpark no Dataproc
┌─────────────────────────────┐
│      BigQuery TRUSTED       │
│  schema unificado · 12 cols │
│  campaigns_unified          │
└─────────────────────────────┘
        ↓  dbt Cloud · 14 models
┌─────────────────────────────┐
│      BigQuery CURATED       │
│  staging → dim → fact → mart│
│  7 KPIs no semantic layer   │
└─────────────────────────────┘
        ↓
   Looker Studio
   Pacing · Performance · Funil
```

Cada camada tem um contrato de qualidade distinto. Nenhuma é pulada.

## Separação de responsabilidades

| Ferramenta | Responsabilidade |
|---|---|
| Streamlit | Ingestão autenticada → BigQuery RAW |
| PySpark | Schema harmonization, tipagem, deduplicação, quarentena |
| dbt | KPIs, modelagem dimensional, semantic layer, testes, docs |

O PySpark não sabe o que é CTR. O dbt não sabe o que é um CSV.

## Governança: surrogate key como base de auditoria

O `taxonomy_id` é o identificador canônico de cada campanha: estável, independente de como ela aparece nas fontes.

```
pmax_awareness_cacaushow_2026   →  id-2026000001
pmx_awareness_cacaushow_2026    →  id-2026000001  (typo)
PMAX Awareness - Cacau Show     →  id-2026000001  (manual)
```

Campanhas sem mapeamento recebem `id-sem-mapeamento` e vão para quarentena. Nunca entram no TRUSTED silenciosamente. O seed `dim_taxonomy_seed.csv` é versionado no Git: quem mudou a classificação, quando e por que: fica no git log.

## Data quality: quarentena em vez de drop

Registros inválidos são isolados com motivo e timestamp. Nunca são apagados.

| Regra | Motivo |
|---|---|
| `spend < 0` | Custo negativo é impossível. Indica bug no export da plataforma. |
| `clicks > impressions` | CTR > 100% é matematicamente impossível |
| `campaign_id IS NULL` | Sem ID, nenhum join futuro é possível |
| `taxonomy_id = 'id-sem-mapeamento'` | Campanha sem classificação de funil. Requer ação humana. |

Destinos: `trusted.quarantine_invalid_campaigns` e `trusted.quarantine_unclassified`.

## dbt: 14 models em 4 camadas

```
staging/      (view)   stg_google_pmax · stg_google_ads · stg_shopping · stg_meta_ads · stg_dv360
dimensions/   (table)  dim_taxonomy · dim_platform · dim_funnel · dim_campaign
facts/        (table)  fct_campaign_daily · fct_platform_performance
marts/        (table)  mart_cross_channel · mart_kpi_summary · mart_pacing
```

Staging são views: baratas de recalcular, refletem atualizações do TRUSTED automaticamente. Facts e marts sao tables: o Looker Studio consulta a cada clique, precisa retornar em milissegundos.

## Semantic layer: 7 KPIs definidos uma vez

| KPI | Fórmula |
|---|---|
| CTR | `clicks / nullif(impressions, 0)` |
| CPC | `spend / nullif(clicks, 0)` |
| CPM | `(spend / nullif(impressions, 0)) * 1000` |
| VTR | `views / nullif(impressions, 0)` |
| CPA | `spend / nullif(conversions, 0)` |
| Pacing | `coalesce(spend_realizado, 0) / budget_mensal` |
| Benchmark | `(ctr / nullif(avg_ctr_platform, 0)) - 1` |

`nullif(x, 0)` evita divisão por zero retornando null. Métrica indefinida, não erro.

## Números reais

| | |
|---|---|
| Linhas processadas no RAW | 10.220 |
| Linhas válidas no TRUSTED | 6.935 |
| Campanhas únicas identificadas | 21 |
| Entradas na taxonomia | 24 |
| Período de dados | Jan/26 → Dez/26 (365 dias) |
| Plataformas unificadas | 5 |
| Models dbt | 14 |
| KPIs centralizados | 7 |

## Estrutura do repositório

```
media-pipeline-2026/
├── ingestion/
│   ├── gerar_dados.py     # gera CSVs com typos intencionais
│   └── app.py             # Streamlit + OAuth PKCE → BigQuery RAW
├── spark/
│   └── trusted.py         # PySpark: RAW → TRUSTED
├── scripts/
│   └── limpar_plano.py    # pré-processa Excel do plano de mídia
├── .env.example
├── requirements.txt
└── README.md
```

O código dbt vive no repositório gerenciado pelo dbt Cloud (`curated_media_pipeline0`).
Os 14 models (staging, dimensions, facts, marts) estão disponíveis no dbt Cloud IDE do projeto.
Para rodar localmente, configure o `profiles.yml` com as credenciais do BigQuery.

## Como executar

```bash
# 1. clone e instale
git clone https://github.com/gustavobrazzo0/media-pipeline-2026
cd media-pipeline-2026

# cria a pasta de dados (está no .gitignore, não está no repo)
mkdir data
mkdir config

# Windows
python -m venv venv
venv\Scripts\activate

# Mac / Linux
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

```bash
# 2. configure variáveis de ambiente
cp .env.example .env
# edite o .env com seus valores de GCP_PROJECT, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET
```

```bash
# 3. gere os dados simulados e o taxonomy_mapping.csv
python ingestion/gerar_dados.py
# cria data/realizado_*.csv e config/taxonomy_mapping.csv
```

```bash
# 4. faça upload para o bucket (script + mapping de taxonomia)
gsutil cp spark/trusted.py gs://media-pipeline-2026-project-089d84bf-7d31-497c-b0a/
gsutil cp config/taxonomy_mapping.csv gs://media-pipeline-2026-project-089d84bf-7d31-497c-b0a/config/

# suba os CSVs para o BigQuery RAW via Streamlit
python -m streamlit run ingestion/app.py
```

```bash
# 5. execute a harmonização no Dataproc
gcloud dataproc clusters start media-pipeline-cluster --region=us-east1
gcloud dataproc jobs submit pyspark gs://media-pipeline-2026-project-089d84bf-7d31-497c-b0a/trusted.py \
  --cluster=media-pipeline-cluster --region=us-east1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  --properties=spark.datasource.bigquery.temporaryGcsBucket=media-pipeline-2026-project-089d84bf-7d31-497c-b0a
gcloud dataproc clusters stop media-pipeline-cluster --region=us-east1
```

```bash
# 6. execute os models dbt
# os models dbt vivem no repositório gerenciado pelo dbt Cloud
# acesse o dbt Cloud IDE ou use o CLI local:
dbt run
dbt test
```

## Stack

Python · PySpark · Google Dataproc · Google BigQuery · dbt Cloud · Streamlit · Google Cloud Storage · IAM · Looker Studio
