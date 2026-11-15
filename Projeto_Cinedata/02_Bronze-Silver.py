# Databricks notebook source
# MAGIC %md
# MAGIC # Atividade 2 - Arquitetura Medalhão
# MAGIC ### Projeto CineData Analytics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 - Ingestão e Preparação (Camada Silver)

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

from pyspark.sql import functions as F, Row
from datetime import datetime

dq_results = []

def dq_check(table_name: str, check_name: str, df, condition):
    """Executa uma checagem de qualidade: conta quantas linhas violam a condição esperada."""
    total = df.count()
    failed = df.filter(~condition).count()
    passed = failed == 0

    dq_results.append(
        Row(table_name=table_name, check_name=check_name, total_rows=total,
            failed_rows=failed, passed=passed, checked_at=datetime.now())
    )
    
    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {table_name} | {check_name} | {failed}/{total} linhas falharam")

def dq_check_unique(table_name: str, check_name: str, df, key_cols: list):
    """Checagem de qualidade específica para unicidade de chave."""
    total = df.count()
    dupes = df.groupBy(*key_cols).count().filter("count > 1").count()
    passed = dupes == 0

    dq_results.append(
        Row(table_name=table_name, check_name=check_name, total_rows=total,
            failed_rows=dupes, passed=passed, checked_at=datetime.now())
    )

    status = "PASS" if passed else "FAIL"
    print(f"[{status}] {table_name} | {check_name} | {dupes} chaves duplicadas de {total} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. tb_movies_info

# COMMAND ----------

df_bronze_info_movies = spark.table(f"{bronze_schema}.tb_movies_info")

df_silver_info_movies = (
    df_bronze_info_movies
    .orderBy(F.col("ingestion_datetime").desc())
    .dropDuplicates(["id"])
    .withColumn("release_date", F.coalesce(
        F.try_to_date(F.col("release_date"), "yyyy-MM-dd"),
        F.try_to_date(F.col("release_date"), "dd/MM/yyyy"),
        F.try_to_date(F.col("release_date"), "MM/dd/yyyy"),
        F.try_to_date(F.col("release_date"), "yyyy/MM/dd"),
        F.try_to_date(F.col("release_date"), "MM-dd-yyyy"))
    )
    .withColumn("status", F.upper(F.trim(F.regexp_replace(F.col("status"), "[-_]", " "))))
    .withColumn("status", 
                F.when(F.col("status") == "RELEASED", "Lançado")
                .when(F.col("status") == "POSTPRODUCTION", "Pós-Produção")
                .when(F.col("status") == "IN PRODUCTION", "Em Produção")
                .when(F.col("status") == "PLANNED", "Planejado")
                .when(F.col("status") == "RUMORED", "Rumores")
                .when(F.col("status") == "CANCELED", "Cancelado")
                .otherwise("Não informado"))
    .select("id", "title", "original_title", "release_date", "runtime", "original_language",  "status", "overview", "tagline")
    .withColumnRenamed("id", "id_filme")
    .withColumnRenamed("title", "titulo")
    .withColumnRenamed("original_title", "titulo_original")
    .withColumnRenamed("release_date", "data_lancamento")
    .withColumnRenamed("runtime", "duracao_minutos")
    .withColumnRenamed("original_language", "idioma_original")
    .withColumnRenamed("status", "status_filme")
    .withColumnRenamed("overview", "sinopse")
    .withColumnRenamed("tagline", "frase_divulgacao")
    .withColumn("ano_lancamento", F.year(F.col("data_lancamento")))
)

valid_status = ["Lançado", "Pós-Produção", "Em Produção", "Planejado", "Rumores", "Cancelado", "Não informado"]

dq_check_unique("silver.tb_info_filmes", "id_filme único", df_silver_info_movies, ["id_filme"])
dq_check("silver.tb_info_filmes", "titulo não nulo", df_silver_info_movies, F.col("titulo").isNotNull())
dq_check("silver.tb_info_filmes", "status_filme em domínio válido", df_silver_info_movies, F.col("status_filme").isin(valid_status))


df_silver_info_movies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_schema}.tb_info_filmes")

display(df_bronze_info_movies.limit(10))
display(df_silver_info_movies.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. tb_movies_financials

# COMMAND ----------

df_bronze_financials = spark.table(f"{bronze_schema}.tb_movies_financials")
df_info_filmes = spark.table(f"{silver_schema}.tb_info_filmes").select("id_filme", "data_lancamento")
df_bronze_cotacao = spark.table(f"{bronze_schema}.tb_cotacao_dolar")

df_cotacao = (
    df_bronze_cotacao
    .withColumn("data_cotacao", F.substring(F.col("dataHoraCotacao"), 1, 10).cast("date"))
    .select("data_cotacao", F.col("cotacaoCompra").cast("decimal(18,4)").alias("cotacao_brl"))
    .distinct()
)

df_silver_metricas = (
    df_bronze_financials
    .withColumnRenamed("id", "id_filme")
    .orderBy(F.col("ingestion_datetime").desc())
    .dropDuplicates(["id_filme"])
    .withColumn("budget", F.regexp_replace(F.col("budget"), "[^0-9.]", ""))
    .withColumn("revenue", F.regexp_replace(F.col("revenue"), "[^0-9.]", ""))
    .withColumn("budget", 
                F.when(
                    F.col("budget").isin("Unknown", "Não Informado", "", ".") |
                    F.col("budget").isNull() |
                    (F.expr("try_cast(budget as decimal(18,2))") <= 0),
                    None           
                )
                .otherwise(F.col("budget").cast("decimal(18,2)"))
    )
    .withColumn("revenue", 
                F.when(
                    F.col("revenue").isin("Unknown", "Não Informado", "", ".") |
                    F.col("revenue").isNull() |
                    (F.expr("try_cast(revenue as decimal(18,2))") <= 0),
                    None           
                )
                .otherwise(F.col("revenue").cast("decimal(18,2)"))
    )
    .join(df_info_filmes, "id_filme", "left")
)

df_cotacao_valida = (
    df_silver_metricas.select("id_filme", "data_lancamento")
    .filter(F.col("data_lancamento").isNotNull())
    .join(df_cotacao, F.col("data_cotacao") <= F.col("data_lancamento"), "inner")
    .groupBy("id_filme")
    .agg(F.max("data_cotacao").alias("data_cotacao"))
)

df_cotacao_final = (
    df_cotacao_valida
    .join(df_cotacao, "data_cotacao", "left")
    .select("id_filme", "cotacao_brl")
)

df_silver_metricas = (
    df_silver_metricas
    .join(df_cotacao_final, "id_filme", "left")
    .withColumn("orcamento_brl", F.round(F.col("budget") * F.col("cotacao_brl"), 2))
    .withColumn("receita_brl", F.round(F.col("revenue") * F.col("cotacao_brl"), 2))
    .withColumn("lucro_usd", F.col("revenue") - F.col("budget"))
    .withColumn("lucro_brl", F.col("receita_brl") - F.col("orcamento_brl"))
    .withColumn("margem_lucro_percentual", 
                F.when((F.col("revenue").isNotNull()) & 
                       (F.col("revenue") > 0) & 
                       (F.col("budget").isNotNull()), 
                F.round((F.col("lucro_usd") / F.col("revenue")) * 100, 2))
        .otherwise(None)
    )
    .select("id_filme", "budget", "revenue", "orcamento_brl", "receita_brl", "lucro_usd", "lucro_brl", "margem_lucro_percentual", )   
    .withColumnRenamed("budget", "orcamento_usd")
    .withColumnRenamed("revenue", "receita_usd")
)

dq_check_unique("silver.tb_financeiro_filmes", "id_filme único", df_silver_metricas, ["id_filme"])
dq_check("silver.tb_financeiro_filmes", "orcamento_usd positivo ou nulo", df_silver_metricas, 
        (F.col("orcamento_usd") >= 0) | F.col("orcamento_usd").isNull())
dq_check("silver.tb_financeiro_filmes", "receita_usd positiva ou nula", df_silver_metricas, 
        (F.col("receita_usd") >= 0) | F.col("receita_usd").isNull())


df_silver_metricas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_schema}.tb_financeiro_filmes")

display(df_bronze_financials.limit(10))
display(df_silver_metricas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. tb_movies_metrics

# COMMAND ----------

# DBTITLE 1,Silver: tb_metricas_engajamento
df_bronze_metricas = spark.table(f"{bronze_schema}.tb_movies_metrics")

pop_str = F.trim(F.col("popularity").cast("string"))
pop_limpa = (
    F.when(
        pop_str.contains(",") & pop_str.contains("."),
        F.regexp_replace(F.regexp_replace(pop_str, r"\.", ""), ",", ".")
    )
    .otherwise(F.regexp_replace(pop_str, ",", "."))
)
pop_limpa = F.regexp_replace(pop_limpa, r"[^0-9.\-]", "")

df_silver_metricas = (
    df_bronze_metricas
    .withColumnRenamed("id", "id_filme")
    # Popularidade
    .withColumn("pop_limpa", pop_limpa)
    .withColumn("popularidade_num", F.expr("try_cast(pop_limpa as double)"))
    .withColumn("popularidade", 
                F.when(F.col("popularidade_num") < 0, None)
                .otherwise(F.col("popularidade_num"))
    )   
    # Nota média - TMDB 
    .withColumn("vote_average", F.expr("try_cast(replace(trim(cast(vote_average as string)), ',', '.') as double)"))
    .withColumn("nota_media_tmdb",
                F.when((F.col("vote_average") < 0) | (F.col("vote_average") > 10), None)
                .otherwise(F.col("vote_average"))
    )
    # Número de votos - TMDB
    .withColumn("vote_count", F.expr("try_cast(replace(trim(cast(vote_count as string)), ',', '.') as double)"))
    .withColumn("qtd_votos_tmdb",
                F.when(F.col("vote_count") < 0, None)
                .otherwise(F.col("vote_count"))
    )
    # Nota média - IMDB
    .withColumn("averageRating", F.expr("try_cast(replace(trim(cast(averageRating as string)), ',', '.') as double)"))
    .withColumn("nota_media_imdb",
                F.when((F.col("averageRating") < 0) | (F.col("averageRating") > 10), None)
                .otherwise(F.col("averageRating"))
    )
    # Número de votos - IMDB
    .withColumn("numVotes", F.expr("try_cast(replace(trim(cast(numVotes as string)), ',', '.') as double)"))
    .withColumn("qtd_votos_imdb",
                F.when(F.col("numVotes") < 0, None)
                .otherwise(F.col("numVotes"))
    )

    .select("id_filme", "popularidade", "nota_media_tmdb", "qtd_votos_tmdb", "nota_media_imdb", "qtd_votos_imdb")
)

dq_check("silver.tb_metricas_engajamento", "popularidade >= 0 ou nula", df_silver_metricas,
         (F.col("popularidade") >= 0) | F.col("popularidade").isNull())
dq_check("silver.tb_metricas_engajamento", "nota_media_tmdb entre 0 e 10 ou nula", df_silver_metricas,
         F.col("nota_media_tmdb").between(0, 10) | F.col("nota_media_tmdb").isNull())
dq_check("silver.tb_metricas_engajamento", "nota_media_imdb entre 0 e 10 ou nula", df_silver_metricas,
         F.col("nota_media_imdb").between(0, 10) | F.col("nota_media_imdb").isNull())
dq_check("silver.tb_metricas_engajamento", "qtd_votos_tmdb >= 0 ou nula", df_silver_metricas,
         (F.col("qtd_votos_tmdb") >= 0) | F.col("qtd_votos_tmdb").isNull())
dq_check("silver.tb_metricas_engajamento", "qtd_votos_imdb >= 0 ou nula", df_silver_metricas,
         (F.col("qtd_votos_imdb") >= 0) | F.col("qtd_votos_imdb").isNull())

df_silver_metricas.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_schema}.tb_metricas_engajamento")

display(df_bronze_metricas.limit(10))
display(df_silver_metricas.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. tb_movies_reviews

# COMMAND ----------

df_bronze_movies_reviews = spark.table(f"{bronze_schema}.tb_movies_reviews")

df_silver_movies_reviews = (
    df_bronze_movies_reviews
    .orderBy(F.col("ingestion_datetime").desc())
    .dropDuplicates(["id", "nome", "nota", "comentario"])
    .withColumn("nota",
                F.when((F.col("nota") < 0) | (F.col("nota") > 10), None)
                .otherwise(F.col("nota"))
    )
    .withColumn("comentario",
                F.when(
                    F.col("comentario").isNull() | (F.trim(F.col("comentario").cast("string")) == ""),
                    F.lit("Sem comentário")
                ).otherwise(F.col("comentario").cast("string"))
    )

    .select("id", "nome", "nota", "comentario")
    .withColumnRenamed("id", "id_filme")
    .withColumnRenamed("nome", "nome_usuario")
    .withColumnRenamed("nota", "nota_usuario")
    .withColumnRenamed("comentario", "comentario_usuario")
)

dq_check_unique(
    "silver.tb_avaliacoes_usuarios",
    "unicidade da combinação (filme, usuário, nota, comentário)",
    df_silver_movies_reviews,
    ["id_filme", "nome_usuario", "nota_usuario", "comentario_usuario"]
)
dq_check(
    "silver.tb_avaliacoes_usuarios",
    "nota_usuario entre 0 e 10 ou nula",
    df_silver_movies_reviews,
    F.col("nota_usuario").between(0, 10) | F.col("nota_usuario").isNull()
)
dq_check(
    "silver.tb_avaliacoes_usuarios",
    "comentario_usuario preenchido (sem nulos ou vazios)",
    df_silver_movies_reviews,
    F.col("comentario_usuario").isNotNull() & (F.trim(F.col("comentario_usuario")) != "")
)

df_silver_movies_reviews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_schema}.tb_avaliacoes_usuarios")

display(df_bronze_movies_reviews.limit(10))
display(df_silver_movies_reviews.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. tb_credits_and_tags (Coluna genres)

# COMMAND ----------

df_bronze_credits = spark.table(f"{bronze_schema}.tb_credits_and_tags")

df_silver_genres = (
    df_bronze_credits
    .withColumn("genres", F.regexp_replace(F.col("genres").cast("string"), ";", ","))
    .withColumn("genres", F.explode(F.split(F.col("genres"), ",")))
    .withColumn("genres", F.trim(F.regexp_replace(F.col("genres"), r"\s+", " ")))
    .filter(
        F.col("id").isNotNull() &
        F.col("genres").isNotNull() &
        (F.col("genres") != "") &
        (F.length(F.col("genres")) <= 35) &
        F.col("genres").rlike(r"^[A-Za-zÀ-ÿ\s\-&]+$") &
        (~F.lower(F.col("genres")).isin("unknown", "null", "none", "nan", "não informado"))
    )
    .dropDuplicates(["id", "genres"])
    .select("id", "genres")
    .withColumnRenamed("id", "id_filme")
    .withColumnRenamed("genres", "genero_filme")
)

dq_check_unique("silver.tb_generos", "unicidade da combinação (id_filme, genero_filme)", df_silver_genres, ["id_filme", "genero_filme"])
dq_check(
    "silver.tb_generos",
    "genero preenchido e sem números",
    df_silver_genres,
    F.col("genero_filme").isNotNull() & (F.trim(F.col("genero_filme")) != "") & (~F.col("genero_filme").rlike(r"\d"))
)

df_silver_genres.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{silver_schema}.tb_generos")

display(df_bronze_credits.limit(10))
display(df_silver_genres.limit(10))