# /transform/who_gho_transform.py
import pandas as pd
from utils.logging import get_logger

log = get_logger(__name__)

def _pick_first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def transform_who(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza a (country, year, indicator, value) y promedia duplicados.
    Devuelve DataFrame ordenado; vacío si entrada vacía.
    """
    if df is None or df.empty:
        log.warning("DataFrame WHO vacío")
        return pd.DataFrame(columns=["country", "year", "indicator", "value"])

    country_col   = _pick_first_present(df, ["country", "SpatialDim", "SpatialDimKey"])
    year_col      = _pick_first_present(df, ["year", "TimeDim", "TimeDimKey"])
    value_col     = _pick_first_present(df, ["value", "NumericValue", "Value"])
    indicator_col = _pick_first_present(df, ["indicator", "Indicator", "IndicatorCode"])

    keep_map = {}
    if country_col:   keep_map[country_col]   = "country"
    if year_col:      keep_map[year_col]      = "year"
    if value_col:     keep_map[value_col]     = "value"
    if indicator_col: keep_map[indicator_col] = "indicator"

    dfx = df[list(keep_map.keys())].rename(columns=keep_map).copy()

    if "indicator" not in dfx.columns:
        dfx["indicator"] = "unknown"

    dfx["year"] = pd.to_numeric(dfx.get("year"), errors="coerce").astype("Int64")
    dfx["value"] = pd.to_numeric(dfx.get("value"), errors="coerce")

    if "country" in dfx.columns:
        dfx["country"] = dfx["country"].astype("string").str.strip().str.upper()
    dfx["indicator"] = dfx["indicator"].astype("string").str.strip()

    dfx = dfx.dropna(subset=["country", "year", "indicator"])
    dfx = dfx.dropna(subset=["value"])

    dfx = (
        dfx.groupby(["country", "year", "indicator"], as_index=False)["value"]
        .mean()
    )

    dfx = dfx.sort_values(["indicator", "country", "year"]).reset_index(drop=True)

    log.info(
        "Transformación WHO: %s filas | indicadores=%s | países=%s",
        len(dfx), dfx["indicator"].nunique(), dfx["country"].nunique(),
    )
    return dfx
