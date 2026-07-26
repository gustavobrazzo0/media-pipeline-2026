# Media Pipeline 2026

Pipeline analitico end-to-end para harmonizacao e modelagem semantica de campanhas multicanal de midia paga, construido do zero no GCP.

## O problema

5 plataformas de midia paga. 5 schemas incompativeis. Nenhum padrao entre elas.

O que Meta chama de `cliques_link`, Google chama de `clicks`. O que DV360 chama de `investimento`, Google Ads chama de `custo`. DV360 nao tem cliques: e comprado por CPM. VTR nao existe em Search.

Alem disso: a mesma campanha aparece como `pmax_awareness_cacaushow_2026` no Google Ads e `pmx_awareness_cacaushow` em outro sistema. E o mesmo nome com um typo, tratado como duas campanhas diferentes.

Sem resolver esses problemas, CTR medio "cross-canal" e mentira.

## A solucao: Medallion Architecture

```
Fontes (5 plataformas de midia paga)
Google PMAX / Google Ads / Google Shopping / Meta Ads / DV360
    |
Streamlit + OAuth 2.0 PKCE
Upload autenticado -> BigQuery RAW
    |
BigQuery RAW
fiel a origem, sem toque
    |  PySpark no Dataproc
BigQuery TRUSTED
schema unificado - 12 colunas
campaigns_unified
    |  dbt Cloud - 14 models
BigQuery CURATED
staging -> dim -> fact -> mart
7 KPIs no semantic layer
    |
Looker Studio
Pacing / Performance / Funil
```

Cada camada tem um contrato de qualidade distinto. Nenhuma e pulada.

## Separacao de responsabilidades

| Ferramenta | Responsabilidade |
|---|---|
| Streamlit | Ingestao autenticada -> BigQuery RAW |
| PySpark | Schema harmonization, tipagem, deduplicacao, quarentena |
| dbt | KPIs, modelagem dimensional, semantic layer, testes, docs |

O PySpark nao sabe o que e CTR. O dbt nao sabe o que e um CSV.

## Governanca: surrogate key como base de auditoria

O `taxonomy_id` e o identificador canonico de cada campanha: estavel, independente de como ela aparece nas fontes.

```
pmax_awareness_cacaushow_2026   ->  id-2026000001
pmx_awareness_cacaushow_2026    ->  id-2026000001  (typo)
PMAX Awareness - Cacau Show     ->  id-2026000001  (manual)
```

Campanhas sem mapeamento recebem `id-sem-mapeamento` e vao para quarentena; nunca entram no TRUSTED silenciosamente. O seed `dim_taxonomy_seed.csv` e versionado no Git: quem mudou a classificacao, quando e por que fica registrado no git log.

## Data quality: quarentena em vez de drop

Registros invalidos sao isolados com motivo e timestamp. Nunca sao apagados.

| Regra | Motivo |
|---|---|
| `spend < 0` | Custo negativo e impossivel, indica bug no export |
| `clicks > impressions` | CTR > 100% e matematicamente impossivel |
| `campaign_id IS NULL` | Sem ID, nenhum join futuro e possivel |
| `taxonomy_id = 'id-sem-mapeamento'` | Campanha sem classificacao, requer acao humana |

Destinos: `trusted.quarantine_invalid_campaigns` e `trusted.quarantine_unclassified`.

## dbt: 14 models em 4 camadas

```
staging/      (view)   stg_google_pmax / stg_google_ads / stg_shopping / stg_meta_ads / stg_dv360
dimensions/   (table)  dim_taxonomy / dim_platform / dim_funnel / dim_campaign
facts/        (table)  fct_campaign_daily / fct_platform_performance
marts/        (table)  mart_cross_channel / mart_kpi_summary / mart_pacing
```

Staging sao views, baratas de recalcular, refletem atualizacoes do TRUSTED automaticamente. Facts e marts sao tables: o Looker Studio consulta a cada clique e precisa retornar em milissegundos.

## Semantic layer: 7 KPIs definidos uma vez

| KPI | Formula |
|---|---|
| CTR | `clicks / nullif(impressions, 0)` |
| CPC | `spend / nullif(clicks, 0)` |
| CPM | `(spend / nullif(impressions, 0)) * 1000` |
| VTR | `views / nullif(impressions, 0)` |
| CPA | `spend / nullif(conversions, 0)` |
| Pacing | `coalesce(spend_realizado, 0) / budget_mensal` |
| Benchmark | `(ctr / nullif(avg_ctr_platform, 0)) - 1` |

`nullif(x, 0)` evita divisao por zero retornando null: metrica indefinida, nao erro.

## Numeros reais

| | |
|---|---|
| Linhas processadas no RAW | 10.220 |
| Linhas validas no TRUSTED | 6.935 |
| Campanhas unicas identificadas | 21 |
| Entradas na taxonomia | 24 |
| Periodo de dados | Jan/26 -> Dez/26 (365 dias) |
| Plataformas unificadas | 5 |
| Models dbt | 14 |
| KPIs centralizados | 7 |

## Estrutura do repositorio

```
media-pipeline-2026/
|-- ingestion/
|   |-- gerar_dados.py     # gera CSVs com typos intencionais
|   +-- app.py             # Streamlit + OAuth PKCE, envia dados para o BigQuery RAW
|-- spark/
|   +-- trusted.py         # PySpark: RAW para TRUSTED
|-- scripts/
|   +-- limpar_plano.py    # pre-processa Excel do plano de midia
|-- .env.example
|-- requirements.txt
+-- README.md
```

O codigo dbt vive no repositorio gerenciado pelo dbt Cloud (`curated_media_pipeline0`).

## Como executar

```bash
# 1. clone e instale
git clone https://github.com/SEU_USUARIO/media-pipeline-2026
cd media-pipeline-2026
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt

# 2. configure variaveis de ambiente
cp .env.example .env
# preencha GCP_PROJECT, OAUTH_CLIENT_ID, OAUTH_CLIENT_SECRET

# 3. gere os dados simulados
python ingestion/gerar_dados.py

# 4. suba os dados para o BigQuery RAW
python -m streamlit run ingestion/app.py

# 5. pre-processe o plano de midia (Excel -> CSVs)
python scripts/limpar_plano.py

# 6. execute a harmonizacao no Dataproc
gcloud dataproc clusters start media-pipeline-cluster --region=us-east1
gsutil cp spark/trusted.py gs://SEU_BUCKET/
gcloud dataproc jobs submit pyspark gs://SEU_BUCKET/trusted.py \
  --cluster=media-pipeline-cluster --region=us-east1 \
  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
  --properties=spark.datasource.bigquery.temporaryGcsBucket=SEU_BUCKET
gcloud dataproc clusters stop media-pipeline-cluster --region=us-east1

# 7. execute os models dbt (no dbt Cloud ou CLI)
dbt run
dbt test
```

## Stack

Python - PySpark - Google Dataproc - Google BigQuery - dbt Cloud - Streamlit - Google Cloud Storage - IAM - Looker Studio
