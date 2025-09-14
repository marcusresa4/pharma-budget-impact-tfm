# /extract/who_gho.py
import requests
import pandas as pd
from extract.constants import COUNTRY_CODES
from utils.logging import get_logger

log = get_logger(__name__)

GHO_BASE = "https://ghoapi.azureedge.net/api"


INDICATORS = {
    "obesity_adults": "NCD_BMI_30A",
    "overweight_adults": "NCD_BMI_25A",
    "bmi_mean": "NCD_BMI_MEAN",
    "diabetes_prevalence": "NCD_DIABETES_PREVALENCE_AGESTD",
    "raised_fpg": "NCD_GLUC_04",
    "fpg_mean": "NCD_GLUC_01",
    "physical_inactivity": "NCD_PAA",
    "che_gdp_pct": "GHED_CHEGDP_SHA2011",
    "oop_share_che": "GHED_OOPSCHE_SHA2011",
}

def _get(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def _fetch_indicator(indicator_code: str, country_codes: list[str]) -> pd.DataFrame:
    url = f"{GHO_BASE}/{indicator_code}"
    data = _get(url).get("value", [])
    if not data:
        return pd.DataFrame(columns=["country", "year", "value"])

    df = pd.DataFrame(data)

    # Columnas típicas y alternativas en GHO
    col_country = "SpatialDim" if "SpatialDim" in df.columns else ("SpatialDimKey" if "SpatialDimKey" in df.columns else None)
    col_year    = "TimeDim"    if "TimeDim"    in df.columns else ("TimeDimKey"    if "TimeDimKey"    in df.columns else None)
    col_value   = "NumericValue" if "NumericValue" in df.columns else ("Value" if "Value" in df.columns else None)

    if not all([col_country, col_year, col_value]):
        return pd.DataFrame(columns=["country", "year", "value"])

    # Filtrar países
    df = df[df[col_country].isin(country_codes)].copy()

    # Normalizar nombres
    df.rename(columns={col_country: "country", col_year: "year", col_value: "value"}, inplace=True)

    # Tipos
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Salida mínima
    return df.dropna(subset=["year", "value"])[["country", "year", "value"]]

def get_diabetes_obesity_data() -> pd.DataFrame:
    """Concatena indicadores WHO definidos y devuelve formato largo estándar."""
    frames = []
    countries = list(COUNTRY_CODES.values())
    log.info("Extracción WHO: %d indicadores", len(INDICATORS))

    for name, code in INDICATORS.items():
        try:
            dfi = _fetch_indicator(code, countries)
            if dfi.empty:
                log.info("Sin filas WHO para %s (%s)", name, code)
                continue
            dfi["indicator"] = name
            frames.append(dfi)
        except requests.HTTPError as e:
            log.error("Error HTTP %s: %s", code, e)
        except Exception as e:
            log.error("Error general %s: %s", code, e)

    if not frames:
        return pd.DataFrame(columns=["country", "year", "indicator", "value"])

    df = pd.concat(frames, ignore_index=True)

    # Mapear código WHO → nombre de país (COUNTRY_CODES: {"Spain":"ESP",...})
    code_to_name = {v: k for k, v in COUNTRY_CODES.items()}
    df["country"] = df["country"].map(code_to_name).fillna(df["country"]).astype("string")

    # Orden estable
    out = df[["country", "year", "indicator", "value"]].sort_values(["indicator", "country", "year"]).reset_index(drop=True)
    log.info("Extracción WHO: %s filas", len(out))
    return out
