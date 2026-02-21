# Tutorial: Replicando el Pipeline ETL de Databricks con CI/CD

Este tutorial explica paso a paso cómo configurar el proyecto `2COVID-19_Complete_ETL_Pipeline` desde cero.

---

## 1. Configuración del Repositorio en GitHub

1.  Crea un nuevo repositorio llamado `2COVID-19_Complete_ETL_Pipeline`.
2.  Crea la siguiente estructura de carpetas:
    *   `notebooks/`: Contiene el código PySpark.
    *   `tests/`: Tests unitarios locales (pytest).
    *   `configs/`: Configuraciones de Jobs.
    *   `.github/workflows/`: Definición del CI/CD.

---

## 2. Los Notebooks de PySpark

Los notebooks deben guardarse con la extensión `.py` y empezar con `# Databricks notebook source`.

### Bronze -> Silver (`notebooks/covid_bronze_to_silver_workflow_opt.py`)
**Punto clave:** Usa detección dinámica de columnas para evitar errores con caracteres especiales (`/`):
```python
cases_1m_col = next((c for c in df_covid_pop.columns if 'Cases' in c and '1M' in c), None)
if cases_1m_col:
    df_covid_pop = df_covid_pop.withColumnRenamed(cases_1m_col, "tot_cases_per_million")
```

---

## 3. Automatización con GitHub Actions (CI/CD)

Crea el archivo `.github/workflows/ci_cd.yml`. Este archivo hace dos cosas:
1.  **CI (Integración Continua):** Corre `pytest` en cada push a cualquier rama.
2.  **CD (Despliegue Continuo):** Si es un push a `main`, sube los notebooks a Databricks y dispara el Job automáticamente.

### Secretos Requeridos
En GitHub (Settings > Secrets > Actions), añade:
*   `DATABRICKS_HOST`: URL de tu workspace (ej: `https://dbc-xxxx.cloud.databricks.com`).
*   `DATABRICKS_TOKEN`: Tu Personal Access Token de Databricks.

---

## 4. Ejecución y Monitoreo

*   **En GitHub:** Ve a la pestaña **Actions** para ver el estado del pipeline.
*   **En Databricks:** Ve a **Workflows > Runs** para ver la ejecución detallada de los notebooks en el cluster.

---

## 5. Mejores Prácticas Incluidas
*   **Idempotencia:** Uso de `.mode("overwrite")` para permitir re-ejecuciones seguras.
*   **Optimización:** Inclusión de `.option("optimizeWrite", "true")` para mejor rendimiento en Delta Lake.
*   **Modularidad:** Separación de capas (Bronze, Silver, Gold) en notebooks distintos.
