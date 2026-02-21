# Databricks notebook source
# MAGIC %md
# MAGIC ## Tests Bronze → Silver | COVID-19 ETL Pipeline
# MAGIC Suite de 9 tests unittest que validan la calidad de los datos transformados.

# COMMAND ----------
import unittest

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col
from datetime import date

# Obtener SparkSession
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
class TestCovidBronzeToSilver(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Configuracion inicial para todos los tests"""
        cls.spark = spark
        cls.VALID_CONTINENTS = ['Europe', 'Asia', 'Africa', 'Oceania', 'Americas']

        # Schema para casos COVID
        schema_cases = StructType([
            StructField("iso_code", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("location", StringType(), True),
            StructField("date", DateType(), True),
            StructField("total_cases", IntegerType(), True),
            StructField("new_cases", IntegerType(), True),
            StructField("total_deaths", IntegerType(), True),
        ])

        # Schema para populacion
        schema_pop = StructType([
            StructField("iso3_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("continent", StringType(), True),
            StructField("population", IntegerType(), True),
            StructField("total_cases", IntegerType(), True),
            StructField("total_deaths", IntegerType(), True),
            StructField("total_cases_per_million", IntegerType(), True),
            StructField("total_deaths_per_million", IntegerType(), True),
            StructField("death_percentage", IntegerType(), True),
            StructField("other_names", StringType(), True),
        ])

        # Datos de prueba - casos
        data_cases = [
            ("COL", "Americas", "Colombia", date(2021, 1, 1), 1000, 50, 25),
            ("BRA", "Americas", "Brazil", date(2021, 1, 2), 5000, 200, 150),
            (None, "Asia", "India", date(2021, 1, 3), 200, 10, 5),
            ("USA", "Americas", "United States", None, 8000, -5, 200),
            ("DEU", "Europe", "Germany", date(2021, 1, 5), -10, 0, 0),
            ("GBR", "Europe", "UK", date(2021, 1, 6), 3000, 100, 80),
            ("KEN", "Africa", "Kenya", date(2021, 1, 7), 500, 20, 10),
        ]
        cls.df_cases_raw = cls.spark.createDataFrame(data_cases, schema_cases)

        # Datos de prueba - poblacion
        data_pop = [
            ("COL", "Colombia", "Americas", 51000000, 5000000, 125000, 98000, 2450, 2, ""),
            ("BRA", "Brazil", "Americas", 214000000, 21000000, 600000, 98000, 2800, 3, ""),
            (None, "India", "Asia", 1400000000, 30000000, 400000, 21428, 285, 1, ""),
            ("USA", "United States", "Americas", 330000000, 45000000, 750000, 136000, 2272, 2, "US"),
            ("DEU", "Germany", "Europe", 83000000, 5000000, 100000, 60240, 1204, 2, ""),
        ]
        cls.df_pop_raw = cls.spark.createDataFrame(data_pop, schema_pop)

        # Aplicar transformaciones Bronze -> Silver (simuladas)
        cls.df_silver_cases = cls.df_cases_raw.filter(
            (col("iso_code").isNotNull()) &
            (col("date").isNotNull()) &
            (col("total_cases") >= 0) &
            (col("continent").isin(cls.VALID_CONTINENTS))
        )
        cls.df_silver_pop = cls.df_pop_raw.filter(
            (col("iso3_code").isNotNull()) &
            (col("country").isNotNull()) &
            (col("population") > 0) &
            (col("continent").isin(cls.VALID_CONTINENTS))
        )

    # Test 1
    def test_remove_nulls(self):
        """Test 1: Validar que se eliminen los registros con valores nulos"""
        null_iso = self.df_silver_cases.filter(col("iso_code").isNull()).count()
        null_date = self.df_silver_cases.filter(col("date").isNull()).count()
        self.assertEqual(null_iso, 0, "Hay registros con iso_code nulo")
        self.assertEqual(null_date, 0, "Hay registros con fecha nula")
        print("[OK] Test de eliminacion de nulls paso correctamente")

    # Test 2
    def test_valid_continents(self):
        """Test 2: Validar que solo se acepten continentes validos"""
        invalid = self.df_silver_cases.filter(
            ~col("continent").isin(self.VALID_CONTINENTS)
        ).count()
        self.assertEqual(invalid, 0, "Hay continentes invalidos")
        print("[OK] Test de continentes validos paso correctamente")

    # Test 3
    def test_reject_negative_cases(self):
        """Test 3: Validar que se rechacen casos negativos"""
        negative = self.df_silver_cases.filter(col("total_cases") < 0).count()
        self.assertEqual(negative, 0, "Hay registros con total_cases negativo")
        print("[OK] Test de rechazo de casos negativos paso correctamente")

    # Test 4
    def test_data_completeness(self):
        """Test 4: Verificar que Silver tenga menos registros que Bronze (filtros aplicados)"""
        bronze_count = self.df_cases_raw.count()
        silver_count = self.df_silver_cases.count()
        self.assertLess(silver_count, bronze_count, "Silver deberia tener menos registros que Bronze")
        print(f"[OK] Bronze: {bronze_count} registros | Silver: {silver_count} registros")

    # Test 5
    def test_no_duplicates(self):
        """Test 5: Verificar que no haya registros duplicados"""
        total = self.df_silver_cases.count()
        distinct = self.df_silver_cases.distinct().count()
        self.assertEqual(total, distinct, "Hay registros duplicados en Silver")
        print("[OK] Test de duplicados paso correctamente")

    # Test 6
    def test_valid_numeric_ranges(self):
        """Test 6: Verificar que los valores numericos sean >= 0"""
        invalid_cases = self.df_silver_cases.filter(col("total_cases") < 0).count()
        invalid_deaths = self.df_silver_cases.filter(col("total_deaths") < 0).count()
        self.assertEqual(invalid_cases, 0, "total_cases negativos encontrados")
        self.assertEqual(invalid_deaths, 0, "total_deaths negativos encontrados")
        print("[OK] Test de rangos numericos paso correctamente")

    # Test 7
    def test_valid_dates(self):
        """Test 7: Verificar que las fechas sean validas"""
        future_dates = self.df_silver_cases.filter(
            col("date") > date.today()
        ).count()
        self.assertEqual(future_dates, 0, "Hay fechas futuras en los datos")
        print("[OK] Test de fechas validas paso correctamente")

    # Test 8
    def test_schema_validation(self):
        """Test 8: Verificar columnas requeridas en los DataFrames Silver"""
        required_cases_cols = ["iso_code", "continent", "location", "date", "total_cases", "new_cases", "total_deaths"]
        required_pop_cols = ["iso3_code", "country", "continent", "population", "total_cases", "total_deaths"]
        for c in required_cases_cols:
            self.assertIn(c, self.df_silver_cases.columns, f"Columna {c} falta en Silver Cases")
        for c in required_pop_cols:
            self.assertIn(c, self.df_silver_pop.columns, f"Columna {c} falta en Silver Pop")
        print("[OK] Test de schema paso correctamente")

    # Test 9
    def test_population_positive(self):
        """Test 9: Verificar que la poblacion sea positiva en Silver Pop"""
        invalid_pop = self.df_silver_pop.filter(col("population") <= 0).count()
        self.assertEqual(invalid_pop, 0, "Hay registros con poblacion <= 0")
        print("[OK] Test de poblacion positiva paso correctamente")


# COMMAND ----------
# Ejecutar los tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestCovidBronzeToSilver)
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# Mostrar resumen
print("\n" + "=" * 70)
print(f"Tests ejecutados: {result.testsRun}")
print(f"Tests exitosos:   {result.testsRun - len(result.failures) - len(result.errors)}")
print(f"Fallos:           {len(result.failures)}")
print(f"Errores:          {len(result.errors)}")
print("=" * 70)

# Fallar el notebook si hay tests fallidos (para que Databricks marque la task como FAILED)
if result.failures or result.errors:
    raise Exception(f"Tests FALLIDOS: {len(result.failures)} fallos, {len(result.errors)} errores")
