from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, LongType

'''
trusted.py: RAW -> TRUSTED
harmoniza 5 plataformas de mídia paga num schema único.
KPIs (CTR, CPM, pacing) ficam no dbt, nao aqui.
'''

GCP_PROJECT     = "project-089d84bf-7d31-497c-b0a"
DATASET_RAW     = "raw"
DATASET_TRUSTED = "trusted"
BUCKET          = "media-pipeline-2026-project-089d84bf-7d31-497c-b0a"

TABELA_SAIDA        = f"{GCP_PROJECT}.{DATASET_TRUSTED}.campaigns_unified"
TABELA_QUARENTENA   = f"{GCP_PROJECT}.{DATASET_TRUSTED}.quarantine_invalid_campaigns"
TABELA_SEM_TAXONOMY = f"{GCP_PROJECT}.{DATASET_TRUSTED}.quarantine_unclassified"


def get_spark():
    return (
        SparkSession.builder
        .appName("media-pipeline-trusted")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def ler_bq(spark, tabela):
    return spark.read.format("bigquery").option("table", tabela).load()


def salvar_bq(df, tabela):
    df.write.format("bigquery").option("table", tabela).mode("overwrite").save()


def normalizar_pmax(spark):
    df = ler_bq(spark, f"{GCP_PROJECT}.{DATASET_RAW}.realizado_google_pmax")
    return (
        df
        .withColumnRenamed("nome_campanha",     "campaign_name_raw")
        .withColumnRenamed("nome_grupo_assets", "ad_group_name")
        .withColumnRenamed("id_campanha",       "campaign_id")
        .withColumnRenamed("custo",             "spend")
        .withColumnRenamed("impressoes",        "impressions")
        .withColumnRenamed("cliques",           "clicks")
        .withColumn("platform",    F.lit("google_pmax"))
        .withColumn("date",        F.to_date("data", "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast(DoubleType()))
        .withColumn("impressions", F.col("impressions").cast(LongType()))
        .withColumn("clicks",      F.col("clicks").cast(LongType()))
        .withColumn("views",       F.lit(None).cast(LongType()))
        .withColumn("reach",       F.lit(None).cast(LongType()))
        .withColumn("conversions", F.lit(None).cast(LongType()))
        .select("date", "platform", "campaign_name_raw", "campaign_id",
                "ad_group_name",
                "spend", "impressions", "clicks", "views", "reach", "conversions")
    )


def normalizar_google_ads(spark):
    df = ler_bq(spark, f"{GCP_PROJECT}.{DATASET_RAW}.realizado_google_ads")
    return (
        df
        .withColumnRenamed("nome_campanha",      "campaign_name_raw")
        .withColumnRenamed("nome_grupo_anuncio", "ad_group_name")
        .withColumnRenamed("id_campanha",        "campaign_id")
        .withColumnRenamed("custo",              "spend")
        .withColumnRenamed("impressoes",         "impressions")
        .withColumnRenamed("cliques",            "clicks")
        .withColumnRenamed("views_100pct",       "views")
        .withColumn("platform",    F.lit("google_ads"))
        .withColumn("date",        F.to_date("data", "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast(DoubleType()))
        .withColumn("impressions", F.col("impressions").cast(LongType()))
        .withColumn("clicks",      F.col("clicks").cast(LongType()))
        # search retorna views=0, mas ausencia de video nao e zero: e null
        .withColumn("views",
            F.when(F.col("views") == 0, F.lit(None))
             .otherwise(F.col("views").cast(LongType()))
        )
        .withColumn("reach",       F.lit(None).cast(LongType()))
        .withColumn("conversions", F.lit(None).cast(LongType()))
        .select("date", "platform", "campaign_name_raw", "campaign_id",
                "ad_group_name",
                "spend", "impressions", "clicks", "views", "reach", "conversions")
    )


def normalizar_shopping(spark):
    df = ler_bq(spark, f"{GCP_PROJECT}.{DATASET_RAW}.realizado_google_shopping")
    return (
        df
        .withColumnRenamed("nome_campanha", "campaign_name_raw")
        .withColumnRenamed("id_campanha",   "campaign_id")
        .withColumnRenamed("custo",         "spend")
        .withColumnRenamed("impressoes",    "impressions")
        .withColumnRenamed("cliques",       "clicks")
        .withColumn("platform",      F.lit("google_shopping"))
        .withColumn("date",          F.to_date("data", "yyyy-MM-dd"))
        .withColumn("spend",         F.col("spend").cast(DoubleType()))
        .withColumn("impressions",   F.col("impressions").cast(LongType()))
        .withColumn("clicks",        F.col("clicks").cast(LongType()))
        .withColumn("ad_group_name", F.lit(None).cast(StringType()))
        .withColumn("views",         F.lit(None).cast(LongType()))
        .withColumn("reach",         F.lit(None).cast(LongType()))
        .withColumn("conversions",   F.lit(None).cast(LongType()))
        .select("date", "platform", "campaign_name_raw", "campaign_id",
                "ad_group_name",
                "spend", "impressions", "clicks", "views", "reach", "conversions")
    )


def normalizar_meta(spark):
    df = ler_bq(spark, f"{GCP_PROJECT}.{DATASET_RAW}.realizado_meta_ads")
    return (
        df
        .withColumnRenamed("nome_campanha",      "campaign_name_raw")
        .withColumnRenamed("nome_anuncio",       "ad_group_name")
        .withColumnRenamed("id_campanha",        "campaign_id")
        .withColumnRenamed("custo",              "spend")
        .withColumnRenamed("impressoes",         "impressions")
        .withColumnRenamed("cliques_link",       "clicks")  # Meta usa cliques_link
        .withColumnRenamed("views_video_100pct", "views")
        .withColumnRenamed("alcance",            "reach")
        .withColumn("platform",    F.lit("meta_ads"))
        .withColumn("date",        F.to_date("data", "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast(DoubleType()))
        .withColumn("impressions", F.col("impressions").cast(LongType()))
        .withColumn("clicks",      F.col("clicks").cast(LongType()))
        .withColumn("views",       F.col("views").cast(LongType()))
        .withColumn("reach",       F.col("reach").cast(LongType()))
        .withColumn("conversions", F.lit(None).cast(LongType()))
        .select("date", "platform", "campaign_name_raw", "campaign_id",
                "ad_group_name",
                "spend", "impressions", "clicks", "views", "reach", "conversions")
    )


def normalizar_dv360(spark):
    '''DV360 usa insertion_order/line_item, mapeado para campaign/ad_group.
    compra por CPM: sem cliques.'''
    df = ler_bq(spark, f"{GCP_PROJECT}.{DATASET_RAW}.realizado_dv360")
    return (
        df
        .withColumnRenamed("insertion_order", "campaign_name_raw")
        .withColumnRenamed("line_item",       "ad_group_name")
        .withColumnRenamed("id_campanha",     "campaign_id")
        .withColumnRenamed("investimento",    "spend")
        .withColumnRenamed("impressoes",      "impressions")
        .withColumnRenamed("views_100pct",    "views")
        .withColumnRenamed("alcance",         "reach")
        .withColumn("platform",    F.lit("dv360"))
        .withColumn("date",        F.to_date("data", "yyyy-MM-dd"))
        .withColumn("spend",       F.col("spend").cast(DoubleType()))
        .withColumn("impressions", F.col("impressions").cast(LongType()))
        .withColumn("clicks",      F.lit(None).cast(LongType()))
        .withColumn("views",       F.col("views").cast(LongType()))
        .withColumn("reach",       F.col("reach").cast(LongType()))
        .withColumn("conversions", F.lit(None).cast(LongType()))
        .select("date", "platform", "campaign_name_raw", "campaign_id",
                "ad_group_name",
                "spend", "impressions", "clicks", "views", "reach", "conversions")
    )


def validar_e_separar(df):
    '''Separa registros estruturalmente inválidos. Retorna (df_ok, df_invalido).

    ATENÇÃO: clicks pode ser NULL (DV360 compra por CPM, sem cliques).
    NULL > x avalia como NULL em SQL/Spark: nem True nem False.
    filter(NULL) descarta a linha; filter(~NULL) também descarta.
    A linha some sem ir para nenhum lado.
    Solução: só aplicar a comparação quando clicks não é NULL.
    '''
    cond_invalido = (
        (F.col("spend") < 0) |
        (F.col("clicks").isNotNull() & (F.col("clicks") > F.col("impressions"))) |
        F.col("campaign_id").isNull()
    )

    df_invalido = (
        df.filter(cond_invalido)
        .withColumn("motivo",
            F.when(F.col("spend") < 0,                     "spend_negativo")
             .when(F.col("clicks") > F.col("impressions"),  "ctr_impossivel")
             .when(F.col("campaign_id").isNull(),           "campaign_id_nulo")
        )
        .withColumn("flagged_at", F.current_timestamp())
    )

    return df.filter(~cond_invalido), df_invalido


def main():
    spark = get_spark()

    dfs = [
        normalizar_pmax(spark),
        normalizar_google_ads(spark),
        normalizar_shopping(spark),
        normalizar_meta(spark),
        normalizar_dv360(spark),
    ]

    df = dfs[0]
    for d in dfs[1:]:
        df = df.union(d)

    df = df.dropDuplicates(["date", "platform", "campaign_id", "ad_group_name"])

    # entity resolution: mapeia campaign_name_raw -> taxonomy_id canônico
    # o CSV contém também typos que apontam para o mesmo id de suas versões corretas
    # ex: pmx_awareness_cacaushow_2026 -> id-2026000001 (igual ao pmax_ correto)
    taxonomy_map = (
        spark.read
        .option("header", "true")
        .csv(f"gs://{BUCKET}/config/taxonomy_mapping.csv")
    )
    df = (
        df.join(taxonomy_map, on="campaign_name_raw", how="left")
        .withColumn("taxonomy_id",
            F.when(F.col("taxonomy_id").isNull(), F.lit("id-sem-mapeamento"))
             .otherwise(F.col("taxonomy_id"))
        )
    )

    df_ok, df_invalido = validar_e_separar(df)

    sem_tax = df_ok.filter(F.col("taxonomy_id") == "id-sem-mapeamento")
    df_ok   = df_ok.filter(F.col("taxonomy_id") != "id-sem-mapeamento")

    total     = df.count()
    n_inv     = df_invalido.count()
    n_sem_tax = sem_tax.count()
    n_ok      = df_ok.count()

    # toda linha que entrou precisa sair em algum destino
    assert total == n_ok + n_inv + n_sem_tax, (
        f"linhas perdidas: total={total:,}, ok={n_ok:,}, "
        f"inv={n_inv:,}, sem_tax={n_sem_tax:,}, "
        f"perdidas={total - n_ok - n_inv - n_sem_tax:,}"
    )

    print(f"total: {total:,} | ok: {n_ok:,} | inválidos: {n_inv:,} | sem taxonomy: {n_sem_tax:,}")

    salvar_bq(df_ok, TABELA_SAIDA)

    if n_inv > 0:
        salvar_bq(df_invalido, TABELA_QUARENTENA)

    if n_sem_tax > 0:
        sem_tax = sem_tax.withColumn("motivo", F.lit("sem_mapeamento_taxonomy"))
        salvar_bq(sem_tax, TABELA_SEM_TAXONOMY)

    spark.stop()


if __name__ == "__main__":
    main()
