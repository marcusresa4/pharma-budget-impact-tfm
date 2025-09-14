from __future__ import annotations
import os
import traceback
from contextlib import contextmanager
from sqlalchemy import text

"""Registro simple de pasos en staging.run_log.
Uso:
    with step_run(engine, "extract_x"):
        ...
Si algo lanza excepción: se marca ERROR y se relanza.
"""

STAGING = os.getenv("STAGING_SCHEMA", "staging")  # esquema destino

CREATE_SCHEMA_STAGING = f"CREATE SCHEMA IF NOT EXISTS {STAGING};"

CREATE_TABLE_RUNLOG = f"""  -- Tabla ligera; ampliable si se necesitan más métricas
CREATE TABLE IF NOT EXISTS {STAGING}.run_log (
  id        BIGSERIAL PRIMARY KEY,
  step      TEXT NOT NULL,
  start_ts  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_ts    TIMESTAMPTZ,
  status    TEXT CHECK (status IN ('OK','ERROR')),
  rows_in   BIGINT,
  rows_out  BIGINT,
  error_msg TEXT
);"""

INSERT_SQL = (
    f"INSERT INTO {STAGING}.run_log (step, start_ts, status) "
    f"VALUES (:step, NOW(), 'OK') RETURNING id;"
)
UPDATE_OK  = (
    f"UPDATE {STAGING}.run_log SET end_ts=NOW(), status='OK', rows_in=:rin, "
    f"rows_out=COALESCE(rows_out, :rout) WHERE id=:id;"
)
UPDATE_ERR = (
    f"UPDATE {STAGING}.run_log SET end_ts=NOW(), status='ERROR', error_msg=:err "
    f"WHERE id=:id;"
)

def ensure_run_log_table(engine):
    """Crea esquema y tabla si faltan (idempotente)."""
    with engine.begin() as conn:
        conn.execute(text(CREATE_SCHEMA_STAGING))
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE_RUNLOG))

@contextmanager
def step_run(engine, step: str, rows_in: int | None = None):
    """Inserta registro inicial, ejecuta bloque y marca fin (OK/ERROR)."""
    ensure_run_log_table(engine)
    with engine.begin() as conn:
        rid = conn.execute(text(INSERT_SQL), {"step": step}).scalar_one()
    try:
        yield rid
    except Exception:
        err = traceback.format_exc(limit=2000)
        with engine.begin() as conn:
            conn.execute(text(UPDATE_ERR), {"id": rid, "err": err})
        raise
    else:
        with engine.begin() as conn:
            conn.execute(text(UPDATE_OK), {"id": rid, "rin": rows_in, "rout": None})

def set_rows_out(engine, run_id: int, rows_out: int | None):
    """Actualiza rows_out para el id dado (último valor prevalece)."""
    with engine.begin() as conn:
        conn.execute(
            text(f"UPDATE {STAGING}.run_log SET rows_out=:rout WHERE id=:id"),
            {"id": run_id, "rout": rows_out},
        )