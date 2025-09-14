"""Config mínima centralizada.

Objetivo: reducir ruido. Solo expone:
    - Rutas de trabajo (DATA_DIR, RAW_DIR, PROCESSED_DIR).
    - Variables de entorno de Postgres (sin validar aquí).
    - Esquemas por defecto.
    - Helper `ensure_dirs()`.

Si una variable es obligatoria se valida fuera (p.ej. al crear el engine).
"""

from __future__ import annotations
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Carga .env si existe

def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    """Env wrapper simple.
    required=True -> RuntimeError si falta o está vacío.
    Devuelve siempre string.
    """
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Falta variable requerida: {name}")
    return (val or "").strip()

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

def ensure_dirs() -> None:
    for d in (DATA_DIR, RAW_DIR, PROCESSED_DIR):
        d.mkdir(parents=True, exist_ok=True)

POSTGRES_URL = os.getenv("POSTGRES_URL", "")
POSTGRES_USER = os.getenv("POSTGRES_USER", "")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

DEFAULT_STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "staging")
DEFAULT_MART_SCHEMA = os.getenv("MART_SCHEMA", "mart")

__all__ = [
    "DATA_DIR",
    "RAW_DIR",
    "PROCESSED_DIR",
    "ensure_dirs",
    "POSTGRES_URL",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "DEFAULT_STAGING_SCHEMA",
    "DEFAULT_MART_SCHEMA",
]
