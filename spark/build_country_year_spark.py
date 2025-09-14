# spark/build_country_year_spark.py
"""Lee long + dim_country, pivota a wide, exporta parquet y tabla PostgreSQL.
"""

from __future__ import annotations
import os
import re
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import functions as F
from spark.session import get_spark
from utils.logging import get_logger

log = get_logger(__name__)

# Indicadores a pivotar (se convierten en columnas)
INDICATORS = [
    "population",
    "life_expectancy_total",
    "diabetes_prevalence_20_79",
    "gdp_per_capita_usd",
    "health_expenditure_pct_gdp",
    "health_expenditure_per_capita_usd",
    "pharma_expenditure_pct_total",
    "pharma_expenditure_per_capita_usd_ppp",
    "hospital_expenditure_pct_total",
    "prevention_expenditure_pct_total",
    "obesity_adults",
    "overweight_adults",
]


# ---------------------------
# Utilidades PostgreSQL
# ---------------------------
def _jdbc_options(schema: str, table_or_sql: str) -> dict:
    """Construye opciones JDBC para lectura desde Postgres."""
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pwd:
        raise RuntimeError("Missing POSTGRES_URL/USER/PASSWORD for Spark JDBC")

    if table_or_sql.strip().lower().startswith("("):
        dbtable = table_or_sql
    else:
        dbtable = f'("{schema}"."{table_or_sql}")'

    return {
        "url": jdbc,
        "dbtable": dbtable,
        "user": user,
        "password": pwd,
        "driver": "org.postgresql.Driver",
        "fetchsize": "20000",
    }


def _parse_jdbc_url(jdbc_url: str) -> Optional[Tuple[str, int, str]]:
    """
    Parse URL estilo JDBC 'jdbc:postgresql://host:port/dbname?params'
    Devuelve (host, port, dbname) o None si no puede parsearse.
    """
    try:
        m = re.match(r"jdbc:postgresql://([^/:?#]+)(?::(\d+))?/([^?]+)", jdbc_url)
        if not m:
            return None
        host = m.group(1)
        port = int(m.group(2) or 5432)
        dbname = m.group(3)
        return host, port, dbname
    except Exception:
        return None


def _ensure_pg_schema(schema: str) -> None:
    """
    Intenta crear el esquema en PostgreSQL usando psycopg2 si está disponible.
    Si psycopg2 no está instalado, simplemente informa y continúa.
    """
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pwd = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pwd:
        print("No hay credenciales PostgreSQL para crear esquema.")
        return

    parsed = _parse_jdbc_url(jdbc)
    if not parsed:
        print(f"No se pudo parsear POSTGRES_URL={jdbc} para creación de esquema.")
        return
    host, port, dbname = parsed

    try:
        import psycopg2  # type: ignore
    except Exception:
        print("psycopg2 no está disponible: omito CREATE SCHEMA (si el schema no existe, el write JDBC fallará).")
        return

    try:
        conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}";')
        conn.close()
        print(f'Esquema "{schema}" verificado/creado en PostgreSQL.')
    except Exception as e:
        print(f'No se pudo asegurar el esquema "{schema}": {e}')


# ---------------------------
# Ejecución principal
# ---------------------------
def run() -> None:
    """Ejecuta el job Spark (join, pivot, exportaciones)."""
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    mart_schema = os.getenv("MART_SCHEMA", "mart")
    export_dir = Path(os.getenv("SPARK_EXPORT_DIR", "./data/warehouse/country_year_wide")).resolve()
    pg_url = os.getenv("POSTGRES_URL")
    pg_user = os.getenv("POSTGRES_USER")
    pg_pwd = os.getenv("POSTGRES_PASSWORD")
    pg_schema = os.getenv("REPORTING_SCHEMA", "reporting")  # permite cambiar schema si lo deseas
    pg_table_fqn = f'"{pg_schema}"."country_year_wide"'

    print(f"ENV → MART_SCHEMA={mart_schema}  SPARK_EXPORT_DIR={export_dir}")
    print("JDBC →", pg_url)

    # 1) Leer long e ISO3 de Postgres (JDBC)
    try:
        print("Paso 1A: leyendo MART.long (country_year_indicators) vía JDBC…")
        long_df = (
            spark.read.format("jdbc")
            .options(
                **_jdbc_options(
                    mart_schema,
                    f'(SELECT country, "year", indicator, value FROM "{mart_schema}"."country_year_indicators") AS src',
                )
            )
            .load()
        )
        print(f"long rows = {long_df.count():,}")
    except Exception as e:
        print("Error leyendo MART.long:", e)
        raise

    try:
        print("Paso 1B: leyendo MART.dim_country vía JDBC…")
        dim_df = (
            spark.read.format("jdbc")
            .options(
                **_jdbc_options(
                    mart_schema,
                    f'(SELECT iso3, country_name FROM "{mart_schema}"."dim_country") AS d',
                )
            )
            .load()
        )
        print(f"dim rows  = {dim_df.count():,}")
    except Exception as e:
        print("Error leyendo dim_country:", e)
        raise

    # 2) Join por nombre de país (alineado con la integración SQL)
    print("Paso 2: join y tipado…")
    df = (
        long_df.join(dim_df, on=long_df.country == dim_df.country_name, how="inner")
        .select(
            "iso3",
            dim_df["country_name"].alias("country_name"),
            long_df["year"].cast("int").alias("year"),
            "indicator",
            long_df["value"].cast("double").alias("value"),
        )
    )
    print(f"tras join rows = {df.count():,}  países={df.select('iso3').distinct().count()}")

    # 3) Pivot → WIDE (máximo por indicador/año para resolver posibles duplicados)
    try:
        print("Paso 3: pivot a WIDE… (esto puede tardar un poco)")
        wide_df = (
            df.groupBy("iso3", "country_name", "year")
            .pivot("indicator", INDICATORS)
            .agg(F.max("value"))
            .orderBy("iso3", "year")
        )
        cnt = wide_df.count()
        yr = wide_df.agg(F.min("year").alias("min"), F.max("year").alias("max")).collect()[0]
        print(f"WIDE rows = {cnt:,}  rango años=[{yr['min']}, {yr['max']}]")
    except Exception as e:
        print("Error en pivot WIDE:", e)
        raise

    # 4) Exportar a Parquet particionado (iso3, year)
    try:
        print(f"Paso 4: escribiendo Parquet particionado → {export_dir}")
        export_dir.mkdir(parents=True, exist_ok=True)
        (
            wide_df.repartition("iso3", "year")
            .write.mode("overwrite")
            .partitionBy("iso3", "year")
            .parquet(str(export_dir))
        )
        print("Parquet escrito.")
    except Exception as e:
        print("Error escribiendo Parquet:", e)
        raise

    # 5) Tabla externa en catálogo SQL embebido de Spark (no PostgreSQL)
    try:
        print("Paso 5: creando tabla externa reporting.country_year_wide_ext (Spark)…")
        spark.sql("CREATE DATABASE IF NOT EXISTS reporting")
        spark.sql("DROP TABLE IF EXISTS reporting.country_year_wide_ext")
        loc = str(export_dir).replace("\\", "/")
        spark.sql(
            "CREATE TABLE reporting.country_year_wide_ext "
            "USING PARQUET "
            f"LOCATION '{loc}'"
        )
        spark.sql("MSCK REPAIR TABLE reporting.country_year_wide_ext")
        spark.catalog.refreshTable("reporting.country_year_wide_ext")
        print("Tabla externa (Spark) creada/refrescada.")
    except Exception as e:
        print("Error creando tabla externa (Spark):", e)
        raise

    # 6) NUEVO: Escribir tabla física en PostgreSQL (visible desde pgAdmin)
    try:
        print(f"Paso 6: escribiendo tabla {pg_schema}.country_year_wide en PostgreSQL…")
        # Intento de asegurar el esquema (si psycopg2 está disponible)
        _ensure_pg_schema(pg_schema)

        if not (pg_url and pg_user and pg_pwd):
            raise RuntimeError("Faltan variables POSTGRES_URL/USER/PASSWORD para escribir en PostgreSQL.")

        props = {"user": pg_user, "password": pg_pwd, "driver": "org.postgresql.Driver"}

        # Orden sugerido: claves primero y luego el resto de columnas
        ordered_cols = ["iso3", "country_name", "year"] + [
            c for c in wide_df.columns if c not in ("iso3", "country_name", "year")
        ]

        (
            wide_df.select(*ordered_cols)
            .write.mode("overwrite")  # sobrescribe la tabla si ya existe
            .jdbc(pg_url, pg_table_fqn, properties=props)
        )

        print(f"Tabla {pg_schema}.country_year_wide escrita en PostgreSQL.")
        print("Sugerencia de índices (ejecutar en pgAdmin si lo deseas):")
        print(
            f"""CREATE INDEX IF NOT EXISTS idx_cyw_iso3_year ON "{pg_schema}"."country_year_wide"(iso3, year);
CREATE INDEX IF NOT EXISTS idx_cyw_year ON "{pg_schema}"."country_year_wide"(year);"""
        )
    except Exception as e:
        print("Error escribiendo en PostgreSQL:", e)
        print("Verifica que el contenedor de Postgres esté accesible, que el esquema exista y que el volumen Parquet no interfiere.")
        raise

    log.info("Spark export → parquet: %s", export_dir)
    log.info("Tabla externa (Spark): reporting.country_year_wide_ext")
    log.info("Tabla PostgreSQL: %s", f"{pg_schema}.country_year_wide")
    print("Job Spark COMPLETADO.")


if __name__ == "__main__":
    run()
