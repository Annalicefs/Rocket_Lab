# Databricks notebook source
# MAGIC %md
# MAGIC # Atividade 2 - Arquitetura Medalhão
# MAGIC ### Projeto CineData Analytics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 - Ingestão e Preparação (Camada Bronze)

# COMMAND ----------

catalog = "cinedata"
bronze_schema_name = "bronze"
silver_schema_name = "silver"
gold_schema_name = "gold"

bronze_schema = f"{catalog}.{bronze_schema_name}"
silver_schema = f"{catalog}.{silver_schema_name}"
gold_schema = f"{catalog}.{gold_schema_name}"

landing_path = f"/Volumes/{catalog}/{bronze_schema_name}/Landing"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# Mapeamento dos caminhos no Volume
path_movies_info = f"{landing_path}/movies_info_TMDB_IMDB.csv"
path_financials = f"{landing_path}/movies_financials_IMDB_TMDB.csv"
path_metrics = f"{landing_path}/movies_metrics_IMDB_TMDB.csv"
path_reviews = f"{landing_path}/movies_reviews.csv"
path_credits_and_tags = f"{landing_path}/credits_and_tags_IMDB_TMDB.csv"

# Leitura pura 
df_movies_info_raw = spark.read.csv(path_movies_info, header=True, inferSchema=True)
df_financials_raw = spark.read.csv(path_financials, header=True, inferSchema=True)
df_metrics_raw = spark.read.csv(path_metrics, header=True, inferSchema=True)
df_reviews_raw = spark.read.csv(path_reviews, header=True, inferSchema=True)
df_credits_and_tags_raw = spark.read.csv(path_credits_and_tags, header=True, inferSchema=True)

# COMMAND ----------

# Gravação com adição do timestamp no momento da escrita
df_movies_info_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{bronze_schema}.tb_movies_info")

df_financials_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{bronze_schema}.tb_movies_financials")

df_metrics_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{bronze_schema}.tb_movies_metrics")

df_reviews_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{bronze_schema}.tb_movies_reviews")

df_credits_and_tags_raw \
    .withColumn("ingestion_datetime", current_timestamp()) \
    .write.format("delta").mode("append") \
    .saveAsTable(f"{bronze_schema}.tb_credits_and_tags")

display(spark.table(f"{bronze_schema}.tb_movies_info").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestão de API do Banco Central

# COMMAND ----------

import requests
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

tabela_destino = f"{bronze_schema}.tb_cotacao_dolar"

# Identificando última data gravada
if spark.catalog.tableExists(tabela_destino):
    df_bronze = spark.table(tabela_destino)

    ultima_data = (
        df_bronze.select(
            F.max(
                F.to_date(F.substring("dataHoraCotacao", 1, 10), "yyyy-MM-dd")
            ).alias("ultima_data")
        )
        .first()["ultima_data"]
    )
else:
    ultima_data = None

# Definindo período de consulta
if ultima_data is None:
    data_inicio = "01-01-2016"
else:
    data_inicio = (
        ultima_data + timedelta(days=1)).strftime("%m-%d-%Y")

# Widgets de data no formato MM-DD-AAAA
dbutils.widgets.text("data_inicio", "01-01-2016", "Data Início (MM-DD-AAAA)")
dbutils.widgets.text("data_fim", datetime.now().strftime("%m-%d-%Y"), "Data Fim (MM-DD-AAAA)")

data_inicio = dbutils.widgets.get("data_inicio")
data_fim = dbutils.widgets.get("data_fim")

url = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/"
    "odata/CotacaoDolarPeriodo"
    f"(dataInicial='{data_inicio}',dataFinalCotacao='{data_fim}')"
    "?$select=dataHoraCotacao,cotacaoCompra"
    "&$format=json"
    "&$orderby=dataHoraCotacao asc"
    "&$top=10000"
)

# Requisição
response = requests.get(url, timeout=60)
response.raise_for_status()

dados_api = response.json().get("value", [])

schema = StructType([
    StructField("dataHoraCotacao", StringType(), True),
    StructField("cotacaoCompra", DoubleType(), True)
])

if dados_api:
    df_cotacao_api = (
        spark.createDataFrame(dados_api, schema)
        .withColumn("ingestion_datetime", F.current_timestamp())
    )

    (
        df_cotacao_api.write
        .format("delta")
        .mode("append")
        .saveAsTable(tabela_destino)
    )

    print(f"Registros recebidos da API: {len(dados_api)}")
else:
    print("A API não retornou cotações para o período informado.")

display(
    spark.table(tabela_destino)
    .orderBy(F.col("dataHoraCotacao").desc())
    .limit(10)
)

# COMMAND ----------

df = spark.table("cinedata.bronze.tb_cotacao_dolar")

# Mostra o total de linhas gravadas
print(f"Total de registros: {df.count()}")

# Mostra os tipos das colunas
df.printSchema()

# Exibe os 20 primeiros registros ordenados pela cotação mais recente
display(df.orderBy("dataHoraCotacao", ascending=False).limit(20))