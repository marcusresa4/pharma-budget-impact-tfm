DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_user') THEN
      CREATE USER test_user WITH PASSWORD 'test_user123';
   END IF;
END
$$;

DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pharma_pipeline') THEN
      CREATE DATABASE pharma_pipeline OWNER test_user;
   END IF;
END
$$;

\c pharma_pipeline

ALTER DATABASE pharma_pipeline OWNER TO test_user;

-- Esquemas necesarios
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION test_user;
CREATE SCHEMA IF NOT EXISTS mart AUTHORIZATION test_user;

GRANT USAGE, CREATE ON SCHEMA staging TO test_user;
GRANT USAGE, CREATE ON SCHEMA mart TO test_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA staging
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO test_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA mart
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO test_user;

-- Tabla de bitácora de pasos
CREATE TABLE IF NOT EXISTS staging.run_log (
  id          BIGSERIAL PRIMARY KEY,
  step        TEXT NOT NULL,
  start_ts    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  end_ts      TIMESTAMPTZ,
  status      TEXT CHECK (status IN ('OK','ERROR')),
  rows_in     BIGINT,
  rows_out    BIGINT,
  error_msg   TEXT
);

CREATE INDEX IF NOT EXISTS run_log_step_ts ON staging.run_log(step, start_ts DESC);
