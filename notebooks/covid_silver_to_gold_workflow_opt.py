# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver → Gold | COVID-19 ETL Pipeline
# MAGIC Agrega datos Silver y construye tablas analíticas Gold.

# COMMAND ----------
# Parametros del workflow
dbutils.widgets.text("environment", "prod", "Ambiente")
dbutils.widgets.text("catalog", "workspace", "Catalogo Unity")
dbutils.widgets.text("schema_silver", "silver", "Schema Silver")
dbutils.widgets.text("schema_gold", "gold", "Schema Gold")
dbutils.widgets.text("run_id", "", "ID de ejecucion")

environment   = dbutils.widgets.get("environment")
catalog       = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")
run_id        = dbutils.widgets.get("run_id")

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum, round, first

spark = SparkSession.builder.appName("COVID19GOLD").getOrCreate()

# COMMAND ----------
# --- Leer tablas Silver ---
df_silver_cases = spark.read.format("delta").table(f"{catalog}.{schema_silver}.wcovid_cases_silver")
df_silver_pop   = spark.read.format("delta").table(f"{catalog}.{schema_silver}.wcovid_pop_silver")

# COMMAND ----------
# --- Gold: Continent Summary ---
gold_continent = df_silver_cases.groupBy(col("continent")).agg(
    countDistinct(col("location")).alias("paises"),
    sum(col("total_cases")).alias("casos_totales"),
    sum(col("total_deaths")).alias("muertes_totales"),
    round((sum(col("total_deaths")) / sum(col("total_cases"))) * 100, 2).alias("tasa_mortalidad_pct")
).filter(col("paises").isNotNull())

gold_continent.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("continent") \
    .option("overwriteSchema", "true") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{schema_gold}.wgold_continent_summary")

# COMMAND ----------
# --- Gold: Country Summary ---
gold_country = df_silver_pop.groupBy(
    col("iso3_code"), col("country"), col("continent")
).agg(
    first(col("population")).alias("poblacion"),
    first(col("total_cases")).alias("casos_totales"),
    first(col("total_deaths")).alias("muertes_totales"),
    first(col("total_cases_per_million")).alias("casos_x_millon"),
    first(col("total_deaths_per_million")).alias("muertes_x_millon"),
    round(first(col("death_percentage")), 2).alias("tasa_mortalidad_pct")
)

gold_country.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{schema_gold}.wgold_country_summary")

# COMMAND ----------
print(f"[OK] Silver → Gold completado | env={environment} | run_id={run_id}")
