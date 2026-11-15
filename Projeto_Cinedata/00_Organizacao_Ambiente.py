# Databricks notebook source
# MAGIC %md
# MAGIC # Atividade 2 - Arquitetura Medalhão
# MAGIC ### Projeto CineData Analytics
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organização do Ambiente

# COMMAND ----------

# Padronização de nomes: <catalog>.<camada>.<tabela>, tudo em snake_case
catalog = "cinedata"
bronze_schema_name = "bronze"
silver_schema_name = "silver"
gold_schema_name = "gold"

bronze_schema = f"{catalog}.{bronze_schema_name}"
silver_schema = f"{catalog}.{silver_schema_name}"
gold_schema = f"{catalog}.{gold_schema_name}"

landing_path = f"/Volumes/{catalog}/{bronze_schema_name}/Landing"

print(f"catalog: {catalog}")
print(f"bronze_schema: {bronze_schema}")
print(f"silver_schema: {silver_schema}")
print(f"gold_schema: {gold_schema}")
print(f"landing_path: {landing_path}")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {bronze_schema}.Landing")

print("Catalog, schemas e volume prontos.")

# COMMAND ----------

expected_files = [
    "movies_info_TMDB_IMDB.csv",
    "movies_financials_IMDB_TMDB.csv",
    "credits_and_tags_IMDB_TMDB.csv",
    "movies_metrics_IMDB_TMDB.csv",
    "movies_reviews.csv",
]

try:
    existing = {f.name for f in dbutils.fs.ls(landing_path)}
except Exception as e:
    existing = set()
    print(f"[ALERTA] Não foi possível listar '{landing_path}'. Faça o upload dos arquivos antes de continuar.\n{e}")

missing = [f for f in expected_files if f not in existing]

if missing:
    print("[PENDENTE] Arquivos ainda não encontrados na landing zone:")
    for f in missing:
        print(f"  - {f}")
else:
    print("[OK] Todos os arquivos esperados estão na landing zone:")
    for f in dbutils.fs.ls(landing_path):
        print(f"  - {f.name} ({f.size/1024:.1f} KB)")