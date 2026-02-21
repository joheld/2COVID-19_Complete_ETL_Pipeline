# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze → Silver | COVID-19 ETL Pipeline
# MAGIC Filtra y limpia datos crudos de COVID-19 hacia la capa Silver.
# COMMAND ----------
# Parametros del workflow
dbutils.widgets.text("environment", "prod", "Ambiente")
dbutils.widgets.text("catalog", "workspace", "Catalogo Unity")
dbutils.widgets.text("schema_bronze", "bronze", "Schema Bronze")
dbutils.widgets.text("schema_silver", "silver", "Schema Silver")
dbutils.widgets.text("run_id", "", "ID de ejecucion")
# Obtener valores
environment   = dbutils.widgets.get("environment")
catalog       = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
run_id        = dbutils.widgets.get("run_id")
# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import StringType, DoubleType, DateType, LongType
spark = SparkSession.builder.appName("COVID19ETL").getOrCreate()
# COMMAND ----------
# --- COVID Cases Bronze → Silver ---
df_covid = spark.read.format("delta").table(f"{catalog}.{schema_bronze}.covid_cases_bronze")

VALID_CONTINENTS = ['Europe', 'Asia', 'Africa', 'Oceania', 'Americas']

df_silver_cases = df_covid.filter(
    (df_covid.iso_code.isNotNull()) &
    (df_covid.date.isNotNull()) &
    (df_covid.total_cases >= 0) &
    (df_covid.continent.isin(VALID_CONTINENTS))
)

df_silver_cases.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("continent") \
    .option("overwriteSchema", "true") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{schema_silver}.wcovid_cases_silver")
# COMMAND ----------
# --- COVID Population Bronze → Silver ---
df_covid_pop = spark.read.format("delta").table(f"{catalog}.{schema_bronze}.covid_pop_bronze")

# Renombrar columna con caracteres especiales antes de operar
df_covid_pop = df_covid_pop.withColumnRenamed("Tot Cases//1M pop", "tot_cases_per_million")

df_silver_pop = df_covid_pop.filter(
    (col("ISO 3166-1 alpha-3 CODE").isNotNull()) &
    (col("Country").isNotNull()) &
    (col("Population") > 0) &
    (col("Continent").isin(VALID_CONTINENTS))
).select(
    col("ISO 3166-1 alpha-3 CODE").alias("iso3_code"),
    col("Country").alias("country"),
    col("Continent").alias("continent"),
    col("Population").alias("population"),
    col("Total Cases").alias("total_cases"),
    col("Total Deaths").alias("total_deaths"),
    col("tot_cases_per_million").alias("total_cases_per_million"),
    col("Tot Deaths/1M pop").alias("total_deaths_per_million"),
    col("Death percentage").alias("death_percentage"),
    col("Other names").alias("other_names")
)

df_silver_pop.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("optimizeWrite", "true") \
    .saveAsTable(f"{catalog}.{schema_silver}.wcovid_pop_silver")
# COMMAND ----------
print(f"[OK] Bronze → Silver completado | env={environment} | run_id={run_id}")
