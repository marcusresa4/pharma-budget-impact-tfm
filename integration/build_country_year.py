import os
from typing import List, Tuple
from sqlalchemy import create_engine, text
from utils.db import sqlalchemy_url_from_jdbc
from utils.config import DEFAULT_STAGING_SCHEMA, DEFAULT_MART_SCHEMA
from utils.logging import get_logger

log = get_logger(__name__)

MART_SCHEMA = os.getenv("MART_SCHEMA", DEFAULT_MART_SCHEMA)
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", DEFAULT_STAGING_SCHEMA)
MART_TABLE = "country_year_indicators"

SOURCES: List[Tuple[str, str]] = [
    ("oecd_sdmx_health", "sdmx"),
    ("worldbank_indicators", "worldbank"),
    ("who_diabetes_obesity", "who"),
]

def _engine():
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pw   = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pw:
        raise RuntimeError("Missing POSTGRES_URL/USER/PASSWORD in env")
    return create_engine(sqlalchemy_url_from_jdbc(jdbc, user, pw), future=True)

def _table_exists(conn, schema: str, table: str) -> bool:
    q = text("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = :schema AND table_name = :table
        LIMIT 1
    """)
    return conn.execute(q, {"schema": schema, "table": table}).first() is not None

def _ensure_mart_table(conn):
    conn.execute(text(f"""
        CREATE SCHEMA IF NOT EXISTS "{MART_SCHEMA}";
        CREATE TABLE IF NOT EXISTS "{MART_SCHEMA}"."{MART_TABLE}" (
            country   TEXT NOT NULL,
            "year"    INT  NOT NULL,
            indicator TEXT NOT NULL,
            value     DOUBLE PRECISION,
            source    TEXT NOT NULL,
            load_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT pk_mart_ind PRIMARY KEY (country, "year", indicator)
        );
    """))
    conn.execute(text(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'idx_mart_country_year' AND n.nspname = '{MART_SCHEMA}'
            ) THEN
                CREATE INDEX idx_mart_country_year ON "{MART_SCHEMA}"."{MART_TABLE}" (country, "year");
            END IF;

            IF NOT EXISTS (
                SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = 'idx_mart_indicator' AND n.nspname = '{MART_SCHEMA}'
            ) THEN
                CREATE INDEX idx_mart_indicator ON "{MART_SCHEMA}"."{MART_TABLE}" (indicator);
            END IF;
        END$$;
    """))

def build_mart(year_min: int = 1990):
    """
    Fusiona staging priorizando SDMX > World Bank > WHO.
    Reemplaza/actualiza valores existentes.
    """
    eng = _engine()
    with eng.begin() as conn:
        _ensure_mart_table(conn)

        existing = [(tbl, src) for tbl, src in SOURCES if _table_exists(conn, STAGING_SCHEMA, tbl)]
        if not existing:
            log.warning("Sin tablas staging para construir MART")
            return

        log.info("Fuentes disponibles: %s", ", ".join(f"{STAGING_SCHEMA}.{t}" for t, _ in existing))

        union_parts: List[str] = []
        for tbl, src in existing:
            union_parts.append(
                f"""SELECT country::TEXT AS country,
                           "year"::INT  AS "year",
                           indicator::TEXT AS indicator,
                           value::DOUBLE PRECISION AS value,
                           '{src}'::TEXT AS source
                    FROM "{STAGING_SCHEMA}"."{tbl}" """
            )
        union_sql = "\nUNION ALL\n".join(union_parts)

        merge_sql = f"""
            WITH unioned AS (
                {union_sql}
            ),
            ranked AS (
                SELECT u.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY country, "year", indicator
                           ORDER BY CASE source
                                        WHEN 'sdmx' THEN 1
                                        WHEN 'worldbank' THEN 2
                                        WHEN 'who' THEN 3
                                        ELSE 4
                                    END
                       ) AS rn
                FROM unioned u
                WHERE country IS NOT NULL AND indicator IS NOT NULL AND "year" IS NOT NULL
                  AND "year" >= {year_min}
            )
            INSERT INTO "{MART_SCHEMA}"."{MART_TABLE}" (country, "year", indicator, value, source, load_ts)
            SELECT country, "year", indicator, value, source, NOW()
            FROM ranked
            WHERE rn = 1
            ON CONFLICT (country, "year", indicator)
            DO UPDATE SET value   = EXCLUDED.value,
                          source  = EXCLUDED.source,
                          load_ts = NOW();
        """
        conn.execute(text(merge_sql))
    log.info("Upsert MART completado en %s.%s", MART_SCHEMA, MART_TABLE)

if __name__ == "__main__":
    build_mart()
