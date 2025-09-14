import pandas as pd
from utils.logging import get_logger

log = get_logger(__name__)

def transform_sdmx(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra columnas SDMX clave y agrega (media) por país/año/indicador.
    """
    if df is None or df.empty:
        log.warning("DataFrame SDMX vacío")
        return pd.DataFrame(columns=["country","year","indicator","value"])

    df = df.rename(columns={
        "LOCATION": "country",
        "TIME_PERIOD": "year",
        "OBS_VALUE": "value",
        "INDICATOR": "indicator",
    })

    keep = [c for c in ["country","year","indicator","value"] if c in df.columns]
    df = df[keep]

    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["country"] = df["country"].astype("string").str.strip().str.upper()
    df["indicator"] = df["indicator"].astype("string").str.strip()

    df = df.dropna(subset=["country","year","indicator"])
    df = df.groupby(["country","year","indicator"], as_index=False)["value"].mean()

    log.info("Transformación SDMX: %s filas", len(df))
    return df
    df = df.groupby(["country","year","indicator"], as_index=False)["value"].mean()

    log.info("Transformación SDMX: %s filas", len(df))
    return df
