import os
import pandas as pd
from sqlalchemy import create_engine, text
from utils.db import sqlalchemy_url_from_jdbc
from utils.config import DEFAULT_STAGING_SCHEMA
from utils.logging import get_logger

log = get_logger(__name__)

TABLE = "worldbank_indicators"
TMP   = "_tmp_worldbank"
SCHEMA = os.getenv("STAGING_SCHEMA", DEFAULT_STAGING_SCHEMA)

def _engine():
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pw   = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pw:
        raise RuntimeError("Missing POSTGRES_URL/USER/PASSWORD in env")
    return create_engine(sqlalchemy_url_from_jdbc(jdbc, user, pw), future=True)

def _ensure_table(conn):
    conn.execute(text(f"""
        CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE}" (
            country    TEXT NOT NULL,
            "year"     INT  NOT NULL,
            indicator  TEXT NOT NULL,
            value      DOUBLE PRECISION,
            load_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_wb_indicators PRIMARY KEY (country, "year", indicator)
        );
    """))

def load_world_bank_to_postgres(df: pd.DataFrame):
    """Upsert con agregación (AVG) y timestamp; ignora DF vacío."""
    if df is None or df.empty:
        log.warning("DataFrame World Bank vacío; nada que cargar")
        return

    eng = _engine()
    with eng.begin() as conn:
        _ensure_table(conn)
        log.info("Subiendo tabla temporal World Bank...")
        df.to_sql(TMP, conn, schema=SCHEMA, if_exists="replace", index=False, method="multi", chunksize=10_000)
        log.info("Upsert World Bank en staging.%s ...", TABLE)
        conn.execute(text(f"""
            INSERT INTO "{SCHEMA}"."{TABLE}" (country, "year", indicator, value, load_ts)
            SELECT country, "year"::INT, indicator, AVG(value) AS value, NOW()
            FROM "{SCHEMA}"."{TMP}"
            GROUP BY country, "year", indicator
            ON CONFLICT (country, "year", indicator)
            DO UPDATE SET value   = EXCLUDED.value,
                          load_ts = NOW();
            DROP TABLE "{SCHEMA}"."{TMP}";
        """))

    log.info("Upsert World Bank completado")
