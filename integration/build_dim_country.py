import os
from sqlalchemy import create_engine, text
from utils.db import sqlalchemy_url_from_jdbc
from utils.config import DEFAULT_MART_SCHEMA
from utils.logging import get_logger
from extract.constants import COUNTRY_CODES

log = get_logger(__name__)

MART_SCHEMA = os.getenv("MART_SCHEMA", DEFAULT_MART_SCHEMA)
TABLE = "dim_country"

def _engine():
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pw   = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pw:
        raise RuntimeError("Missing POSTGRES_URL/USER/PASSWORD in env")
    return create_engine(sqlalchemy_url_from_jdbc(jdbc, user, pw), future=True)

def build_dim_country():
    """Upsert de la dimensión de países desde el diccionario estático."""
    eng = _engine()
    with eng.begin() as conn:
        # Asegurar esquema y tabla
        conn.execute(text(f"""
            CREATE SCHEMA IF NOT EXISTS "{MART_SCHEMA}";
            CREATE TABLE IF NOT EXISTS "{MART_SCHEMA}"."{TABLE}" (
                iso3          CHAR(3) PRIMARY KEY,
                country_name  TEXT NOT NULL
            );
        """))
        # Upsert desde constantes (normaliza a MAYÚSCULAS)
        values = ",".join([f"('{iso3}', '{name.upper()}')" for name, iso3 in COUNTRY_CODES.items()])
        if not values:
            log.warning("Sin COUNTRY_CODES para construir dim_country")
            return
        conn.execute(text(f"""
            INSERT INTO "{MART_SCHEMA}"."{TABLE}" (iso3, country_name)
            VALUES {values}
            ON CONFLICT (iso3)
            DO UPDATE SET country_name = EXCLUDED.country_name;
        """))
    log.info("Construida %s.%s con %d países", MART_SCHEMA, TABLE, len(COUNTRY_CODES))
    
if __name__ == "__main__":
    build_dim_country()
