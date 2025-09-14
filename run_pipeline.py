# Secuencia general del pipeline:
# extract -> transform -> load (staging) -> integración MART -> publicación opcional Spark

from __future__ import annotations

import os
import sys
from typing import Callable, Any
from dotenv import load_dotenv

from utils.logging import setup_logging, get_logger
from utils.db import sqlalchemy_url_from_jdbc
from sqlalchemy import create_engine, text

# Import específico de cada dominio (extract / transform / load)
from extract.who_gho import get_diabetes_obesity_data
from extract.world_bank import fetch_world_bank_data
from extract import sdmx as sdmx_mod

from transform.who_gho_transform import transform_who
from transform.world_bank_transform import transform_worldbank_population
from transform.sdmx_transform import transform_sdmx

from load.who_gho_load import load_who_gho_to_postgres
from load.world_bank_load import load_world_bank_to_postgres
from load.sdmx_load import load_sdmx_to_postgres

# Integraciones SQL finales
from integration.build_dim_country import build_dim_country
from integration.build_country_year import build_mart, SOURCES
from integration.build_country_year_wide import build_country_year_wide

# Publicación Spark (cuando está disponible)
try:
    from spark.build_country_year_spark import run as spark_publish
except Exception:
    spark_publish = None

from utils.runlog import step_run, ensure_run_log_table, set_rows_out
from extract.constants import COUNTRY_CODES

load_dotenv()
log = get_logger(__name__)

POSTGRES_URL = os.getenv("POSTGRES_URL")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
STAGING = os.getenv("STAGING_SCHEMA", "staging")
MART = os.getenv("MART_SCHEMA", "mart")

YEAR_MIN = int(os.getenv("YEAR_MIN", "1990"))  # recorte inferior global de año


def _engine():
    """Crea el engine SQLAlchemy (valida credenciales básicas)."""
    if not POSTGRES_URL or not POSTGRES_USER or not POSTGRES_PASSWORD:
        raise RuntimeError("Faltan POSTGRES_URL/USER/PASSWORD en el entorno")
    sa_url = sqlalchemy_url_from_jdbc(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)
    return create_engine(sa_url)


def _ensure_schemas(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {STAGING};"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {MART};"))


def _etl(
    engine,
    prefix: str,
    extract_fn: Callable[[], Any],
    transform_fn: Callable[[Any], Any],
    load_fn: Callable[[Any], None],
):
    """
    Ejecuta el patrón E-T-L para una fuente.
    Registra pasos en run_log y aplica filtro YEAR_MIN si hay columna 'year'.
    """
    # Extract
    with step_run(engine, f"extract_{prefix}") as rid:
        raw = extract_fn()
        try:
            set_rows_out(engine, rid, len(raw))
        except Exception:
            pass
    # Transform
    with step_run(engine, f"transform_{prefix}", rows_in=len(raw) if hasattr(raw, "__len__") else None) as rid:
        tdf = transform_fn(raw)
        if hasattr(tdf, "__getitem__") and hasattr(tdf, "columns") and "year" in getattr(tdf, "columns", []):
            try:
                tdf = tdf[tdf["year"] >= YEAR_MIN]
            except Exception:
                pass
        try:
            set_rows_out(engine, rid, len(tdf))
        except Exception:
            pass
    # Load
    with step_run(engine, f"load_{prefix}", rows_in=len(tdf) if hasattr(tdf, "__len__") else None) as rid:
        load_fn(tdf)
        try:
            set_rows_out(engine, rid, len(tdf))
        except Exception:
            pass


def _count(engine, schema: str, table: str) -> int | None:
    try:
        with engine.begin() as conn:
            return conn.execute(text(f'SELECT count(*) FROM "{schema}"."{table}"')).scalar() or 0
    except Exception:
        return None


def main():
    """Orquestación completa con manejo controlado de fallos."""
    setup_logging()
    try:
        engine = _engine()
    except Exception:
        log.exception("No se pudo crear el engine de SQLAlchemy")
        sys.exit(2)

    _ensure_schemas(engine)
    ensure_run_log_table(engine)

    try:
        _etl(engine, "who", get_diabetes_obesity_data, transform_who, load_who_gho_to_postgres)
        _etl(engine, "worldbank", fetch_world_bank_data, transform_worldbank_population, load_world_bank_to_postgres)
        _etl(engine, "sdmx", sdmx_mod.get_health_expenditure_data, transform_sdmx, load_sdmx_to_postgres)
    except Exception:
        log.exception("Fallo en alguna etapa de Extract/Transform/Load")
        sys.exit(1)

    try:
        # Construcción de dimensiones y hechos (versión larga y ancha)
        with step_run(engine, "integration_dim_country", rows_in=len(COUNTRY_CODES)) as rid:
            build_dim_country()
            cnt_dim = _count(engine, MART, "dim_country")
            if cnt_dim is not None:
                set_rows_out(engine, rid, cnt_dim)

        rows_in_long = sum((_count(engine, STAGING, tbl) or 0) for tbl, _src in SOURCES)
        with step_run(engine, "integration_long", rows_in=rows_in_long) as rid:
            build_mart(year_min=YEAR_MIN)
            cnt_long = _count(engine, MART, "country_year_indicators")
            if cnt_long is not None:
                set_rows_out(engine, rid, cnt_long)

        rows_in_wide = sum((_count(engine, MART, t) or 0) for t in ("dim_country", "country_year_indicators"))
        with step_run(engine, "integration_wide_sql", rows_in=rows_in_wide) as rid:
            build_country_year_wide()
            cnt_wide = _count(engine, MART, "country_year_wide")
            if cnt_wide is not None:
                set_rows_out(engine, rid, cnt_wide)
        log.info("MART listo: dim_country, tabla long y derivada wide (SQL).")
    except Exception:
        log.exception("Fallo en la integración SQL de MART")
        sys.exit(1)

    # Publicación Spark (omisible vía variable de entorno)
    if os.getenv("SKIP_SPARK_PUBLISH", "0") == "1":
        log.info("Publicación Spark deshabilitada por configuración.")
    elif spark_publish:
        try:
            with step_run(engine, "publish_spark"):
                spark_publish()
            log.info("Publicación Spark completada.")
        except Exception:
            log.exception("Publicación Spark fallida; se continúa sin abortar.")
    else:
        log.warning("PySpark no disponible: se omite fase de publicación.")

    log.info("Pipeline OK")


if __name__ == "__main__":
    main()
