import os
from urllib.parse import urlparse
from sqlalchemy import create_engine

"""Conversión JDBC → URL SQLAlchemy y creación de engine Postgres.

NOTA: hay un bloque duplicado al final (posible error de merge). No se elimina aquí por la restricción de no tocar lógica.
"""

def sqlalchemy_url_from_jdbc(jdbc_url: str, user: str, password: str) -> str:
        """Convierte URL JDBC al formato psycopg2 aceptado por SQLAlchemy."""
        parsed = urlparse(jdbc_url.replace("jdbc:", ""))
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        db   = parsed.path.lstrip("/") or "postgres"
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def get_engine():
    """Devuelve engine con pool_pre_ping; falla si faltan credenciales.

    Lee credenciales del entorno y valida presencia. Usa pool_pre_ping para
    evitar conexiones zombi y future=True para API 2.0.
    """
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pwd  = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pwd:
        raise RuntimeError("Faltan POSTGRES_URL/USER/PASSWORD en el entorno")

    url = sqlalchemy_url_from_jdbc(jdbc, user, pwd)
    return create_engine(url, pool_pre_ping=True, future=True)

def sqlalchemy_url_from_jdbc(jdbc_url: str, user: str, password: str) -> str:
        """Convierte una URL JDBC en la forma aceptada por SQLAlchemy (psycopg2)."""
        parsed = urlparse(jdbc_url.replace("jdbc:", ""))
        host = parsed.hostname or "localhost"
        port = parsed.port or 5432
        db   = parsed.path.lstrip("/") or "postgres"
        return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def get_engine():
    """Devuelve un Engine SQLAlchemy configurado para Postgres.

    Lee credenciales del entorno y valida presencia. Usa pool_pre_ping para
    evitar conexiones zombi y future=True para API 2.0.
    """
    jdbc = os.getenv("POSTGRES_URL")
    user = os.getenv("POSTGRES_USER")
    pwd  = os.getenv("POSTGRES_PASSWORD")
    if not jdbc or not user or not pwd:
        raise RuntimeError("Faltan POSTGRES_URL/USER/PASSWORD en el entorno")

    url = sqlalchemy_url_from_jdbc(jdbc, user, pwd)
    return create_engine(url, pool_pre_ping=True, future=True)
