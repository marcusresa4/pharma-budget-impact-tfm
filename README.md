# Pipeline de Impacto Presupuestario en Farmacia

**Proyecto TFM** (Trabajo de Fin de Máster) para construir una **pipeline reproducible** que integra fuentes públicas (OMS, Banco Mundial, OCDE/SDMX) y publica un **modelo de datos MART** en **Postgres** (en formatos *long* y *wide*), junto con una **exportación analítica en Parquet** (particionada por `iso3, year`) usando **Spark (opcional)**.

---

## Descripción general

**Objetivo:** crear un *data mart* coherente por país y año con indicadores demográficos, económicos y de salud (obesidad/diabetes y gasto sanitario), listo para análisis e informes.

**Salidas principales**

- **Postgres (MART relacional)**
    - `mart.dim_country` (dimensión país con `iso3`).
    - `mart.country_year_indicators` (**long**) — consolidado `(country, year, indicator, value)` con prioridad de fuente.
    - `mart.country_year_wide` (**wide**) — tabla pivoteada con columnas fijas (ej. `population`, `gdp_per_capita_usd`, `obesity_adults`, …).

- **Data Lake (Spark, opcional)**
    - Parquet particionado por **`iso3, year`**: `data/warehouse/country_year_wide/`.
    - Tabla externa en el catálogo SQL embebido de Spark: `reporting.country_year_wide_ext`.

---

## Tecnologías principales

- **Python** 3.10+ (pandas, requests, SQLAlchemy, python-dotenv, psycopg2)
- **PostgreSQL**
- **Apache Spark (PySpark)** — **opcional**, ejecutable vía **Docker Compose**
- **Docker / Docker Compose** (para Spark)
- **Makefile** (atajos locales; opcional en Windows)

---

## Fuentes de datos integradas

- **OMS – Observatorio Mundial de la Salud (GHO):** indicadores de diabetes y obesidad.
- **Banco Mundial:** población y demografía, macroeconomía, empleo, etc.
- **OCDE – SDMX Salud:** gasto sanitario (total, hospitalario, prevención, farmacéutico).

(El proyecto está diseñado para ser fácilmente ampliable con nuevas fuentes.)

---

## Alcance geográfico

La dimensión de países (`mart.dim_country`) cubre 47 países con foco europeo para asegurar comparabilidad de indicadores de salud y socioeconómicos:

- Predominan países de Europa (UE + Europa Occidental, Central y del Este, Balcanes y países nórdicos).
- Incluye micro‑estados (Andorra, Mónaco, San Marino) y estados europeos no miembros de la UE (Noruega, Suiza, Serbia, etc.).
- Incorpora países transcontinentales o en la frontera Europa‑Asia: Turquía, Federación Rusa, Georgia, Azerbaiyán, Armenia.
- Moldavia ("República de Moldavia") y Ucrania aportan cobertura del límite oriental europeo.

Este conjunto refleja un foco alineado con la Región Europea de la OMS y permite mantener un equilibrio entre amplitud geográfica y calidad de cobertura de datos para los indicadores seleccionados.

---

## Flujo de la pipeline (lógica paso a paso)

1) **Configuración**  
     `.env` define credenciales y parámetros (ver sección “Configuración”).

2) **Extracción** (`extract.*`)  
     - OMS: descarga 9 indicadores de diabetes/obesidad.  
     - Banco Mundial: ~20 indicadores (población, esperanza de vida, PIB, etc.).  
     - SDMX/OCDE: gasto sanitario y relacionados.

3) **Transformación** (`transform.*`)  
     - Normaliza a un esquema común: `country`, `year`, `indicator`, `value`.  
     - Filtra por `YEAR_MIN`.

4) **Carga a staging** (`load.*`)  
     Tablas en Postgres (esquema `staging`):
     - `staging.who_diabetes_obesity`, `staging.worldbank_indicators`, `staging.oecd_sdmx_health`.

5) **Dimensión país** (`integration/build_dim_country.py`)  
     Crea/actualiza `mart.dim_country` con 47 países (incluye `iso3`).

6) **MART “long”** (`integration/build_country_year.py`)  
     - **Unión** de las tres fuentes normalizadas.  
     - **Prioridad** por calidad/completitud: `sdmx > worldbank > who`.  
     - Para cada `(country, year, indicator)` elige la fuente de mayor prioridad (window + `ROW_NUMBER`).  
     - **Upsert idempotente** con `ON CONFLICT (country, year, indicator)`.

7) **MART “wide”** (`integration/build_country_year_wide.py`)  
     - Join con `dim_country` para añadir `iso3` y pivot a wide.

8) **Publicación Spark (opcional)** (`spark/build_country_year_spark.py`)  
     - Lee JDBC (`mart.country_year_indicators` y `dim_country`).  
     - Repite el pivot y **escribe Parquet particionado** por `(iso3, year)` en `data/warehouse/country_year_wide/`.  
     - Registra `reporting.country_year_wide_ext` en el catálogo SQL embebido.  
     - Ejecutar manualmente con Docker Compose (ver “Spark opcional”).

> Auditoría de ejecución: cada fase (extract / transform / load / integración / publicación) registra un paso en `staging.run_log` con métricas (filas de entrada/salida y timestamps) para trazabilidad e idempotencia operativa.

---

## Configuración

Crea un archivo `.env` en la raíz:

```ini
# Postgres (si Spark en Docker accede a tu Postgres local, usa host.docker.internal)
POSTGRES_URL=jdbc:postgresql://localhost:5432/pharma_pipeline
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Esquemas
STAGING_SCHEMA=staging
MART_SCHEMA=mart

# Filtro de año para la pipeline
YEAR_MIN=1990

# Controla la publicación Spark dentro de la pipeline (0 = publica, 1 = omite)
SKIP_SPARK_PUBLISH=1

# (Opcional) Spark
SPARK_WAREHOUSE_DIR=./data/spark-warehouse
SPARK_JARS_PACKAGES=org.postgresql:postgresql:42.7.4
SPARK_EXPORT_DIR=./data/warehouse/country_year_wide
```

> Si ejecutas Spark dentro de Docker y tu Postgres está en tu **máquina**, usa `jdbc:postgresql://host.docker.internal:5432/...` en `.env.docker` (ver abajo).

---

## Ejecución (de cero a dashboard)

> Requisitos: Python 3.10+, Postgres accesible, `requirements.txt` instalado.  
> Windows: puedes usar PowerShell. Si no tienes `make`, ejecuta los comandos explícitamente.

0) **Inicialización de Postgres (opcional)**

Si necesitas aprovisionar la base de datos desde cero (usuario, BD, esquemas y permisos), usa `sql/init_db.sql`.

**Qué hace:**
- Crea el usuario `test_user` (si no existe).
- Crea la BD `pharma_pipeline` y la asigna a `test_user`.
- Crea los esquemas `staging` y `mart` con permisos apropiados.
- Define privilegios por defecto y crea `staging.run_log`.

**Ejecuta con `psql`:**
```bash
psql -h localhost -U postgres -f sql/init_db.sql
```

1) **Instala dependencias**
```bash
python -m pip install -r requirements.txt
```

2) **Ejecuta la pipeline (ETL + MART SQL)**  
```bash
python run_pipeline.py
```
- Los logs mostrarán: ETLs, integración `dim_country`/`long`/`wide`, y (si `SKIP_SPARK_PUBLISH=1`) “skipping Spark publication”.

3) **Verifica resultados en Postgres**  
          - Tablas esperadas: `staging.*` (3 tablas de origen), `mart.dim_country`, `mart.country_year_indicators`, `mart.country_year_wide` (si no has deshabilitado la parte SQL).  
          - Consulta rápida (ejemplos):  
               `SELECT COUNT(*) FROM mart.country_year_indicators;`  
               `SELECT * FROM mart.country_year_wide LIMIT 5;`

4) **(Opcional) Publicación Spark en Docker**  
     - Asegúrate de tener **Docker** y **Docker Compose**.
     - Crea `.env.docker` (puedes copiar `.env` y ajustar el host):
         ```ini
         POSTGRES_URL=jdbc:postgresql://host.docker.internal:5432/pharma_pipeline
         POSTGRES_USER=postgres
         POSTGRES_PASSWORD=postgres
         STAGING_SCHEMA=staging
         MART_SCHEMA=mart
         SPARK_WAREHOUSE_DIR=./data/spark-warehouse
         SPARK_JARS_PACKAGES=org.postgresql:postgresql:42.7.4
         SPARK_EXPORT_DIR=./data/warehouse/country_year_wide
         ```
     - Ejecuta Spark con Docker Compose:
         ```bash
         docker compose -f docker-compose.spark.yml down -v
         docker compose -f docker-compose.spark.yml up --build --abort-on-container-exit
         ```
     - **Qué valida el éxito:**  
         - Logs con `Job Spark COMPLETED.`  
         - Carpeta `data/warehouse/country_year_wide/` con subcarpetas `iso3=XXX/year=YYYY/part-*.parquet` y sin `_temporary/`.  
         - (Opcional) archivo `_SUCCESS`.  
         - Tabla externa consultable desde Spark: `reporting.country_year_wide_ext`.

---

## Idempotencia y orden correcto

- **MART long**: `ON CONFLICT` mantiene la integridad (`(country, year, indicator)`).
- **Spark Parquet**: `mode("overwrite")` y `partitionBy("iso3","year")` aseguran publicaciones limpias.

---

## Autor

**Marc Resa** – Máster en Big Data, Data Science y AI.

---

## Licencia

Uso académico / docente para el TFM. Ajustar antes de redistribuir.

