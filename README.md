# 2COVID-19_Complete_ETL_Pipeline

![CI/CD](https://github.com/joheld/2COVID-19_Complete_ETL_Pipeline/actions/workflows/ci_cd.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-Serverless-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-enabled-green)

Pipeline ETL completo de COVID-19 implementado con **PySpark**, **Delta Lake** y **Databricks Workflows**. Arquitectura Medallion (Bronze -> Silver -> Gold) con CI/CD automatizado via GitHub Actions.

---

## Arquitectura

```
BRONZE (raw)          SILVER (clean)               GOLD (analytics)
-----------           ---------------              ----------------
covid_cases_bronze -> wcovid_cases_silver  --+
                                              +--> wgold_continent_summary
covid_pop_bronze   -> wcovid_pop_silver    --+
                                              +--> wgold_country_summary
```

### Capas
| Capa | Tabla | Descripcion |
|------|-------|-------------|
| Bronze | `covid_cases_bronze` | Datos crudos de casos COVID-19 |
| Bronze | `covid_pop_bronze` | Datos crudos de poblacion por pais |
| Silver | `wcovid_cases_silver` | Casos filtrados y validados, particionados por continente |
| Silver | `wcovid_pop_silver` | Poblacion limpia con columnas renombradas |
| Gold | `wgold_continent_summary` | Agregacion de KPIs por continente |
| Gold | `wgold_country_summary` | Resumen estadistico por pais |

---

## Estructura del Repo

```
2COVID-19_Complete_ETL_Pipeline/
|-- .github/
|   +-- workflows/
|       +-- ci_cd.yml              # GitHub Actions CI/CD
|-- notebooks/
|   |-- covid_bronze_to_silver_workflow_opt.py   # Task 1: Bronze -> Silver
|   |-- covid_silver_to_gold_workflow_opt.py     # Task 2: Silver -> Gold
|   +-- covid_testing_bronze_to_silver.py        # Tests Databricks (9 unittest)
|-- tests/
|   +-- test_pipeline_local.py     # Tests locales pytest (22 tests, sin Spark)
|-- configs/
|   +-- job_config.json            # Configuracion del Databricks Job
|-- .gitignore
|-- README.md
+-- requirements.txt
```

---

## CI/CD con GitHub Actions

### Flujo
```
Push a cualquier branch
        |
        v
  [JOB 1: test]
  - Instala pytest + pandas
  - Corre 22 tests locales (sin Spark)
        |
     PASA? ----NO----> Workflow falla (deploy bloqueado)
        |
       SI
        |
  Es push a main?
        |
       SI
        v
  [JOB 2: deploy]
  - Instala Databricks CLI
  - Despliega los 3 notebooks al Workspace
  - Lanza Job ID 762347435635648
  - Espera resultado (max 30 min)
  - Falla si Job fallo
```

### Setup requerido

Agrega estos 2 secrets en **Settings > Secrets and variables > Actions**:

| Secret | Valor |
|--------|-------|
| `DATABRICKS_HOST` | `https://dbc-6100158f-36ab.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Tu Personal Access Token de Databricks |

#### Como generar el token en Databricks:
1. Ir a **Settings** > **Developer** > **Access tokens**
2. Click en **Generate new token**
3. Ponerle nombre `github-actions` y expiracion 90 dias
4. Copiar el token y pegarlo en el secret de GitHub

---

## Tests

### Tests locales (pytest) - corren en GitHub Actions
```bash
pip install -r requirements.txt
pytest tests/test_pipeline_local.py -v
```

22 tests organizados en 4 clases:
- `TestBronzeToSilverCases` (9 tests) - Valida filtros sobre casos COVID
- `TestBronzeToSilverPop` (5 tests) - Valida filtros sobre poblacion
- `TestSilverToGold` (5 tests) - Valida logica de agregacion Gold
- `TestConstants` (3 tests) - Valida configuracion del pipeline

### Tests Databricks (unittest) - corren en el notebook
9 tests con datos de prueba usando PySpark in-memory:
- T1: Eliminacion de nulls
- T2: Continentes validos
- T3: Rechazo de casos negativos
- T4: Completitud de datos
- T5: Sin duplicados
- T6: Rangos numericos validos
- T7: Fechas validas (no futuras)
- T8: Schema validation
- T9: Poblacion positiva

---

## Databricks Job

| Campo | Valor |
|-------|-------|
| **Job ID** | `762347435635648` |
| **Workspace** | `https://dbc-6100158f-36ab.cloud.databricks.com` |
| **Compute** | Serverless (autoscaling) |
| **Task 1** | `Opt_bronze_to_silver` |
| **Task 2** | `Opt_Silver_to_Gold` (depende de Task 1) |
| **Catalogo** | `workspace` |
| **Schemas** | `bronze` -> `silver` -> `gold` |

---

## Stack Tecnologico

- **Apache Spark / PySpark** - procesamiento distribuido
- **Delta Lake** - formato de tabla ACID con optimizeWrite
- **Databricks Serverless** - compute sin gestion de clusters
- **Unity Catalog** - governance y lineage de datos
- **GitHub Actions** - CI/CD automatizado
- **pytest + pandas** - testing local sin dependencias pesadas

---

## Autor

**joojle95@gmail.com** | Databricks Free Edition | Costa Rica

<!-- Last updated: 2026-02-20 - CI/CD pipeline fully operational -->
