# integration/build_country_year_wide.py
import os
from sqlalchemy import create_engine, text
from utils.db import sqlalchemy_url_from_jdbc
from utils.logging import get_logger

log = get_logger(__name__)

MART  = os.getenv("MART_SCHEMA", "mart")

INDICATORS_WIDE = [
    ("population",                                "population"),
    ("life_expectancy_total",                     "life_expectancy_total"),
    ("diabetes_prevalence_20_79",                 "diabetes_prevalence_20_79"),
    ("gdp_per_capita_usd",                        "gdp_per_capita_usd"),
    ("health_expenditure_pct_gdp",                "health_expenditure_pct_gdp"),
    ("health_expenditure_per_capita_usd",         "health_expenditure_per_capita_usd"),
    ("pharma_expenditure_pct_total",              "pharma_expenditure_pct_total"),
    ("pharma_expenditure_per_capita_usd_ppp",     "pharma_expenditure_per_capita_usd_ppp"),
    ("hospital_expenditure_pct_total",            "hospital_expenditure_pct_total"),
    ("prevention_expenditure_pct_total",          "prevention_expenditure_pct_total"),
    ("obesity_adults",                            "obesity_adults"),
    ("overweight_adults",                         "overweight_adults"),
]

def _engine():
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pw   = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pw:
        raise RuntimeError("Faltan POSTGRES_URL/USER/PASSWORD en el entorno")
    return create_engine(sqlalchemy_url_from_jdbc(jdbc, user, pw), future=True)

def _create_wide_table(conn):
    # Genera SQL de pivote usando agregaciones condicionales
    select_parts = []
    for src_name, col_name in INDICATORS_WIDE:
        select_parts.append(
            f"MAX(CASE WHEN indicator = '{src_name}' THEN value END) AS {col_name}"
        )
    select_block = ",\n                ".join(select_parts)

    sql = f"""
        CREATE SCHEMA IF NOT EXISTS "{MART}";
        DROP TABLE IF EXISTS "{MART}"."country_year_wide";
        CREATE TABLE "{MART}"."country_year_wide" AS
        WITH src AS (
            SELECT d.iso3,
                   d.country_name,
                   l.year,
                   l.indicator,
                   l.value
            FROM "{MART}".dim_country d
            JOIN "{MART}"."country_year_indicators" l
              ON l.country = d.country_name
        )
        SELECT
            iso3,
            country_name,
            year::INT AS year,
            {select_block}
        FROM src
        GROUP BY iso3, country_name, year
        ORDER BY iso3, year;
        CREATE INDEX IF NOT EXISTS idx_cyw_iso3_year ON "{MART}"."country_year_wide"(iso3, year);
    """
    conn.execute(text(sql))

def build_country_year_wide():
    """Regenera tabla wide según lista fija de indicadores."""
    eng = _engine()
    with eng.begin() as conn:
        _create_wide_table(conn)
    log.info("Recreada %s.country_year_wide", MART)

if __name__ == "__main__":
    build_country_year_wide()
