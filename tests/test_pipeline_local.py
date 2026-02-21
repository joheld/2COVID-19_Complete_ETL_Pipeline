"""
tests/test_pipeline_local.py

Tests locales del pipeline COVID-19 que corren con pytest en GitHub Actions.
NO requieren Spark ni conexion a Databricks - usan pandas/Python puro.
Estos tests validan la logica de transformacion de manera rapida y ligera.
"""
import pytest
import pandas as pd
from datetime import date, timedelta

# Constante central de continentes validos (igual que en los notebooks)
VALID_CONTINENTS = ['Europe', 'Asia', 'Africa', 'Oceania', 'Americas']


# ============================================================
# FIXTURES - Datos de prueba reutilizables
# ============================================================

@pytest.fixture
def sample_covid_cases():
    """DataFrame de casos COVID con datos buenos y malos mezclados."""
    return pd.DataFrame({
        "iso_code":    ["COL", "BRA", None,  "USA",  "DEU",  "GBR",  "KEN"],
        "date":        ["2021-01-01", "2021-01-02", "2021-01-03", None, "2021-01-05", "2021-01-06", "2021-01-07"],
        "total_cases": [1000, 5000, 200, 8000, -10, 3000, 500],
        "new_cases":   [50,   200,  10,  -5,    0,  100,   20],
        "total_deaths":[25,   150,   5,  200,    0,   80,   10],
        "continent":   ["Americas", "Americas", "Asia", "Americas", "Europe", "Europe", "Africa"],
        "location":    ["Colombia", "Brazil", "India", "United States", "Germany", "UK", "Kenya"],
    })


@pytest.fixture
def sample_covid_pop():
    """DataFrame de poblacion con datos buenos y malos mezclados."""
    return pd.DataFrame({
        "ISO 3166-1 alpha-3 CODE": ["COL",  "BRA",       None,  "USA"],
        "Country":                 ["Colombia", "Brazil", None,  "United States"],
        "Population":              [51000000, 214000000,   0,    330000000],
        "Continent":               ["Americas", "Americas", "Asia", "Americas"],
        "Total Cases":             [5000000,  21000000,    100,  45000000],
        "Total Deaths":            [125000,   600000,       1,   750000],
        "Tot Cases//1M pop":       [98000,    98000,         0,  136000],
        "Tot Deaths/1M pop":       [2450,     2800,          0,   2272],
        "Death percentage":        [2.5,      2.86,        1.0,   1.67],
        "Other names":             ["",       "",           "",    "US"],
    })


@pytest.fixture
def silver_cases(sample_covid_cases):
    """Simula la transformacion Bronze -> Silver sobre casos COVID."""
    df = sample_covid_cases.copy()
    return df[
        df["iso_code"].notna() &
        df["date"].notna() &
        (df["total_cases"] >= 0) &
        df["continent"].isin(VALID_CONTINENTS)
    ].reset_index(drop=True)


@pytest.fixture
def silver_pop(sample_covid_pop):
    """Simula la transformacion Bronze -> Silver sobre poblacion."""
    df = sample_covid_pop.copy()
    filtered = df[
        df["ISO 3166-1 alpha-3 CODE"].notna() &
        df["Country"].notna() &
        (df["Population"] > 0) &
        df["Continent"].isin(VALID_CONTINENTS)
    ].copy()
    filtered = filtered.rename(columns={
        "ISO 3166-1 alpha-3 CODE": "iso3_code",
        "Country": "country",
        "Continent": "continent",
        "Population": "population",
        "Total Cases": "total_cases",
        "Total Deaths": "total_deaths",
        "Tot Cases//1M pop": "total_cases_per_million",
        "Tot Deaths/1M pop": "total_deaths_per_million",
        "Death percentage": "death_percentage",
        "Other names": "other_names",
    })
    return filtered.reset_index(drop=True)


# ============================================================
# TESTS BRONZE -> SILVER (casos)
# ============================================================

class TestBronzeToSilverCases:

    def test_remove_null_iso_code(self, silver_cases):
        """T1: Registros con iso_code nulo deben ser eliminados."""
        assert silver_cases["iso_code"].isna().sum() == 0

    def test_remove_null_date(self, silver_cases):
        """T2: Registros con fecha nula deben ser eliminados."""
        assert silver_cases["date"].isna().sum() == 0

    def test_reject_negative_total_cases(self, silver_cases):
        """T3: total_cases negativos deben ser rechazados."""
        assert (silver_cases["total_cases"] < 0).sum() == 0

    def test_valid_continents_only(self, silver_cases):
        """T4: Solo continentes del catalogo valido deben pasar."""
        assert silver_cases["continent"].isin(VALID_CONTINENTS).all()

    def test_silver_has_fewer_rows_than_bronze(self, sample_covid_cases, silver_cases):
        """T5: Silver debe tener menos registros que Bronze (filtros aplicados)."""
        assert len(silver_cases) < len(sample_covid_cases)

    def test_no_duplicate_rows(self, silver_cases):
        """T6: No debe haber filas duplicadas en Silver."""
        assert silver_cases.duplicated().sum() == 0

    def test_required_columns_exist(self, silver_cases):
        """T7: Columnas clave deben existir en Silver Cases."""
        required = ["iso_code", "date", "total_cases", "total_deaths", "continent", "location"]
        for col in required:
            assert col in silver_cases.columns, f"Columna faltante: {col}"

    def test_dates_not_in_future(self, silver_cases):
        """T8: Fechas no deben ser futuras."""
        today_str = str(date.today())
        future = silver_cases[silver_cases["date"] > today_str]
        assert len(future) == 0

    def test_total_deaths_non_negative(self, silver_cases):
        """T9: total_deaths debe ser >= 0."""
        assert (silver_cases["total_deaths"] < 0).sum() == 0


# ============================================================
# TESTS BRONZE -> SILVER (poblacion)
# ============================================================

class TestBronzeToSilverPop:

    def test_remove_null_iso_code(self, silver_pop):
        """T10: Registros con iso3_code nulo deben ser eliminados."""
        assert silver_pop["iso3_code"].isna().sum() == 0

    def test_remove_null_country(self, silver_pop):
        """T11: Registros con country nulo deben ser eliminados."""
        assert silver_pop["country"].isna().sum() == 0

    def test_population_positive(self, silver_pop):
        """T12: Poblacion debe ser > 0."""
        assert (silver_pop["population"] <= 0).sum() == 0

    def test_column_rename_applied(self, silver_pop):
        """T13: Columnas deben estar renombradas correctamente."""
        expected_cols = ["iso3_code", "country", "continent", "population",
                         "total_cases", "total_deaths", "total_cases_per_million",
                         "total_deaths_per_million", "death_percentage"]
        for col in expected_cols:
            assert col in silver_pop.columns, f"Columna faltante: {col}"

    def test_valid_continents_pop(self, silver_pop):
        """T14: Solo continentes validos en Silver Pop."""
        assert silver_pop["continent"].isin(VALID_CONTINENTS).all()


# ============================================================
# TESTS SILVER -> GOLD (logica de agregacion)
# ============================================================

class TestSilverToGold:

    def test_continent_aggregation_columns(self):
        """T15: Agregacion por continente debe producir columnas correctas."""
        df = pd.DataFrame({
            "continent":    ["Americas", "Americas", "Europe", "Europe"],
            "location":     ["Colombia", "Brazil",   "Germany", "UK"],
            "total_cases":  [1000, 5000, 3000, 2000],
            "total_deaths": [25,   150,   60,   40],
        })
        agg = df.groupby("continent").agg(
            paises=("location", "nunique"),
            casos_totales=("total_cases", "sum"),
            muertes_totales=("total_deaths", "sum"),
        ).reset_index()
        agg["tasa_mortalidad_pct"] = round(
            (agg["muertes_totales"] / agg["casos_totales"]) * 100, 2
        )
        assert "tasa_mortalidad_pct" in agg.columns
        assert "paises" in agg.columns
        assert "casos_totales" in agg.columns

    def test_continent_country_count(self):
        """T16: Conteo de paises por continente debe ser correcto."""
        df = pd.DataFrame({
            "continent": ["Americas", "Americas", "Europe"],
            "location":  ["Colombia", "Brazil",   "Germany"],
            "total_cases": [1000, 5000, 3000],
            "total_deaths": [25, 150, 60],
        })
        agg = df.groupby("continent").agg(paises=("location", "nunique")).reset_index()
        americas_row = agg[agg["continent"] == "Americas"]
        assert americas_row["paises"].values[0] == 2

    def test_mortality_rate_calculation(self):
        """T17: Tasa de mortalidad se calcula correctamente."""
        total_cases  = 10000
        total_deaths = 250
        rate = round((total_deaths / total_cases) * 100, 2)
        assert rate == 2.5

    def test_country_summary_no_duplicate_iso(self):
        """T18: Summary de paises no debe tener iso3_code duplicados."""
        df = pd.DataFrame({
            "iso3_code":  ["COL", "BRA", "COL"],
            "country":    ["Colombia", "Brazil", "Colombia"],
            "population": [51000000, 214000000, 51000000],
        })
        result = df.drop_duplicates(subset=["iso3_code"])
        assert result["iso3_code"].duplicated().sum() == 0

    def test_gold_tables_not_empty(self):
        """T19: Las tablas Gold no deben estar vacias dado un Silver valido."""
        silver = pd.DataFrame({
            "continent":    ["Americas", "Europe"],
            "location":     ["Colombia", "Germany"],
            "total_cases":  [1000, 3000],
            "total_deaths": [25, 60],
        })
        gold = silver.groupby("continent").agg(
            paises=("location", "nunique"),
            casos_totales=("total_cases", "sum"),
        ).reset_index()
        assert len(gold) > 0


# ============================================================
# TESTS DE CONSTANTES Y CONFIGURACION
# ============================================================

class TestConstants:

    def test_valid_continents_count(self):
        """T20: Debe haber exactamente 5 continentes validos."""
        assert len(VALID_CONTINENTS) == 5

    def test_valid_continents_includes_americas(self):
        """T21: Americas debe estar en los continentes validos."""
        assert "Americas" in VALID_CONTINENTS

    def test_valid_continents_no_duplicates(self):
        """T22: No debe haber continentes duplicados."""
        assert len(VALID_CONTINENTS) == len(set(VALID_CONTINENTS))
