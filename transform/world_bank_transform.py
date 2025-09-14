import pandas as pd
from utils.logging import get_logger

log = get_logger(__name__)

def transform_worldbank_population(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y estandariza datos WB al formato largo estándar.
    Promedia duplicados por clave.
    """
    if df is None or df.empty:
        log.warning("DataFrame World Bank vacío")
        return pd.DataFrame(columns=["country","year","indicator","value"])

    # Verifica columnas esperadas
    expected_cols = ["country", "year", "indicator", "value"]
    if not all(col in df.columns for col in expected_cols):
        log.error("Faltan columnas esperadas en datos World Bank")
        return pd.DataFrame(columns=expected_cols)

    df = df[expected_cols].copy()

    # Conversión de tipos
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["country"] = df["country"].astype("string").str.strip().str.upper()
    df["indicator"] = df["indicator"].astype("string").str.strip()

    # Eliminar nulos
    df = df.dropna(subset=["country","year","indicator","value"])
    
    # Deduplicar promediando por (country, year, indicator)
    df = df.groupby(["country","year","indicator"], as_index=False)["value"].mean()

    log.info("Transformación World Bank: %s filas", len(df))
    return df
