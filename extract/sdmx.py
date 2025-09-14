"""
Extracción SDMX (OECD) con intentos de fallback (unidad/categoría) y salida homogénea.
"""
import requests
import pandas as pd
from extract.constants import COUNTRY_CODES
from typing import Optional, Union, List
from utils.logging import get_logger

log = get_logger(__name__)


def fetch_sdmx_indicator(
    unit_measure: Union[str, List[str]],
    indicator_name: str,
    sha_category: Optional[Union[str, List[str]]] = None,
) -> pd.DataFrame:
    """Intenta combinaciones (unidad, categoría) hasta encontrar datos válidos."""
    # URL base para el dataset SHA (estructura fija en la instancia pública SDMX)
    base_url = "https://sdmx.oecd.org/public/rest/data/OECD.ELS.HD,DSD_SHA@DF_SHA,"

    # Códigos de país válidos (solo aquellos cuyo valor es un código de 3 letras OECD)
    oecd_country_codes = {k: v for k, v in COUNTRY_CODES.items() if len(v) == 3}

    # Mantener referencia: podría usarse en consultas específicas (no necesario en el patrón actual)
    countries_param = "+".join(oecd_country_codes.values())  # Nota: reservado para filtros futuros

    # Parámetros temporales: inicio de serie y formato JSON SDMX "compact"
    time_params = "?startPeriod=2015&dimensionAtObservation=AllDimensions&format=jsondata"

    # Lista de fallback para unidades y categorías SHA
    unit_measures = [unit_measure] if isinstance(unit_measure, str) else list(unit_measure)
    sha_categories: List[Optional[str]] = (
        [sha_category] if isinstance(sha_category, str) else (list(sha_category) if sha_category else [None])
    )

    # Probar todas las combinaciones hasta encontrar datos
    for um in unit_measures:
        for cat in sha_categories:
            # Clave SDMX:
            # Estructura (segmento relevante): .A.EXP_HEALTH.{UNIT}._T..{CAT|_T}.._T...
            # Los '_T' representan agregaciones totales en otras dimensiones no usadas.
            query_key = f".A.EXP_HEALTH.{um}._T..{(cat if cat else '_T')}.._T..."
            url = f"{base_url}/{query_key}{time_params}"

            try:
                response = requests.get(url, timeout=30)
                response.raise_for_status()
                data = response.json()

                # Validación mínima de estructura SDMX
                if "data" in data and "dataSets" in data["data"] and data["data"]["dataSets"]:
                    dataset = data["data"]["dataSets"][0]
                    structure = data["data"]["structures"][0]

                    # Extraer dimensiones de interés (REF_AREA / TIME_PERIOD)
                    ref_area_dim = None
                    time_dimension = None
                    for dim in structure["dimensions"]["observation"]:
                        if dim["id"] == "REF_AREA":
                            ref_area_dim = dim
                        elif dim["id"] == "TIME_PERIOD":
                            time_dimension = dim

                    if ref_area_dim and time_dimension and "observations" in dataset:
                        all_data = []
                        code_to_name = {v: k for k, v in oecd_country_codes.items()}

                        # Cada key es una tupla indexada en formato "i:j:k:...:t"
                        for key, values in dataset["observations"].items():
                            key_parts = key.split(":")
                            # Asumimos REF_AREA está en la primera posición y TIME_PERIOD en la última
                            ref_area_index = int(key_parts[0])
                            time_index = int(key_parts[-1])

                            # Validación de límites protectora
                            if (ref_area_index < len(ref_area_dim["values"]) and
                                    time_index < len(time_dimension["values"])):

                                country_code = ref_area_dim["values"][ref_area_index]["id"]
                                year = time_dimension["values"][time_index]["id"]
                                value = values[0] if values and values[0] is not None else None

                                # Solo conservar si hay valor y el país está mapeado
                                if value is not None and country_code in code_to_name:
                                    all_data.append({
                                        "country": code_to_name[country_code],
                                        "year": int(year),
                                        "value": float(value),
                                        "indicator": indicator_name
                                    })

                        # Retorno inmediato en la primera combinación exitosa
                        if all_data:
                            return pd.DataFrame(all_data)
            except Exception:
                # Silencioso: se intenta la siguiente combinación; el logging final cubrirá el fallo global.
                continue
    log.error("Error al obtener datos SDMX para %s", indicator_name)
    return pd.DataFrame()


def fetch_sdmx_health_expenditure() -> pd.DataFrame:
    """
    Gasto sanitario total como porcentaje del PIB (% PIB).
    Unidad: PT_B1GQ
    Indicador: health_expenditure_pct_gdp
    """
    return fetch_sdmx_indicator("PT_B1GQ", "health_expenditure_pct_gdp")


def fetch_sdmx_health_expenditure_per_capita() -> pd.DataFrame:
    """
    Gasto sanitario per cápita en euros PPP.
    Unidad: EUR_PPP_PS
    Indicador: health_expenditure_per_capita_eur_ppp
    """
    return fetch_sdmx_indicator("EUR_PPP_PS", "health_expenditure_per_capita_eur_ppp")


def fetch_sdmx_pharma_expenditure_per_capita() -> pd.DataFrame:
    """
    Gasto farmacéutico per cápita (USD PPP).
    Unidad: USD_PPP_PS
    Categorías fallback: HC51 / HC5_1 / HC.5.1
    Indicador: pharma_expenditure_per_capita_usd_ppp
    """
    return fetch_sdmx_indicator(
        unit_measure=["USD_PPP_PS"],
        indicator_name="pharma_expenditure_per_capita_usd_ppp",
        sha_category=["HC51", "HC5_1", "HC.5.1"],
    )


def fetch_sdmx_pharma_expenditure_pct_total() -> pd.DataFrame:
    """
    Gasto farmacéutico como % del gasto sanitario corriente.
    Unidad: PT_EXP_HLTH
    Categorías fallback: HC51 / HC5_1 / HC.5.1
    Indicador: pharma_expenditure_pct_total
    """
    return fetch_sdmx_indicator(
        unit_measure=["PT_EXP_HLTH"],
        indicator_name="pharma_expenditure_pct_total",
        sha_category=["HC51", "HC5_1", "HC.5.1"],
    )


def fetch_sdmx_hospital_expenditure_pct_total() -> pd.DataFrame:
    """
    Gasto hospitalario como % del gasto sanitario corriente.
    Unidad: PT_EXP_HLTH
    Categorías fallback: HC3 / HC.3
    Indicador: hospital_expenditure_pct_total
    """
    return fetch_sdmx_indicator(
        unit_measure=["PT_EXP_HLTH"],
        indicator_name="hospital_expenditure_pct_total",
        sha_category=["HC3", "HC.3"],
    )


def fetch_sdmx_prevention_expenditure_pct_total() -> pd.DataFrame:
    """
    Gasto en prevención como % del gasto sanitario corriente.
    Unidad: PT_EXP_HLTH
    Categorías fallback: HC6 / HC.6
    Indicador: prevention_expenditure_pct_total
    """
    return fetch_sdmx_indicator(
        unit_measure=["PT_EXP_HLTH"],
        indicator_name="prevention_expenditure_pct_total",
        sha_category=["HC6", "HC.6"],
    )


def fetch_sdmx_obesity_or_overweight_population() -> pd.DataFrame:
    """
    Población con obesidad o sobrepeso (medido vs autodeclarado).
    Dataset: HEALTH_LVNG_BW
    Medidas:
        - MSRD: medido
        - SR  : autodeclarado
    Se generan indicadores:
        - obesity_or_overweight_population_measured
        - obesity_or_overweight_population_self_reported
    """
    base_url = "https://sdmx.oecd.org/public/rest/data/OECD.ELS.HD,DSD_HEALTH_LVNG@DF_HEALTH_LVNG_BW,"
    query_key = ".A..._T..MSRD+SR"
    time_params = "?startPeriod=2010&dimensionAtObservation=AllDimensions&format=jsondata"
    url = f"{base_url}/{query_key}{time_params}"

    # Filtro de países (códigos de 3 letras)
    oecd_country_codes = {k: v for k, v in COUNTRY_CODES.items() if len(v) == 3}
    code_to_name = {v: k for k, v in oecd_country_codes.items()}

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data or "dataSets" not in data["data"] or not data["data"]["dataSets"]:
            return pd.DataFrame()

        dataset = data["data"]["dataSets"][0]
        structure = data["data"]["structures"][0]
        obs_dims = structure["dimensions"]["observation"]

        # Localiza posiciones para REF_AREA, TIME_PERIOD y la dimensión con MSRD/SR.
        ref_area_pos = time_pos = None
        meas_pos = None
        ref_area_dim = time_dim = meas_dim = None

        for i, dim in enumerate(obs_dims):
            if dim["id"] == "REF_AREA":
                ref_area_pos, ref_area_dim = i, dim
            elif dim["id"] == "TIME_PERIOD":
                time_pos, time_dim = i, dim
            else:
                # Heurística: tomar la primera dimensión que incluya MSRD o SR
                ids = {v.get("id") for v in dim.get("values", [])}
                if "MSRD" in ids or "SR" in ids:
                    meas_pos, meas_dim = i, dim

        if ref_area_pos is None or time_pos is None or "observations" not in dataset:
            return pd.DataFrame()

        all_rows: List[dict] = []
        for key, values in dataset["observations"].items():
            parts = key.split(":")
            try:
                ra_idx = int(parts[ref_area_pos])
                t_idx = int(parts[time_pos])
                meas_code = None
                if meas_pos is not None:
                    m_idx = int(parts[meas_pos])
                    if m_idx < len(meas_dim["values"]):
                        meas_code = meas_dim["values"][m_idx]["id"]
            except Exception:
                continue

            if ra_idx >= len(ref_area_dim["values"]) or t_idx >= len(time_dim["values"]):
                continue

            country_code = ref_area_dim["values"][ra_idx]["id"]
            if country_code not in code_to_name:
                continue

            year = time_dim["values"][t_idx]["id"]
            val = values[0] if values and values[0] is not None else None
            if val is None:
                continue

            kind = "measured" if meas_code == "MSRD" else ("self_reported" if meas_code == "SR" else (meas_code.lower() if meas_code else "unknown"))
            indicator = f"obesity_or_overweight_population_{kind}"

            all_rows.append({
                "country": code_to_name[country_code],
                "year": int(year),
                "value": float(val),
                "indicator": indicator
            })

        return pd.DataFrame(all_rows)
    except Exception:
        log.error("Error al obtener datos SDMX para obesity_or_overweight_population")
        return pd.DataFrame()


def fetch_sdmx_hospital_expenditure_per_capita() -> pd.DataFrame:
    """
    Gasto hospitalario per cápita (USD PPP).
    Unidad: USD_PPP_PS
    Categorías fallback: HC3 / HC.3
    Indicador: hospital_expenditure_per_capita_usd_ppp
    """
    return fetch_sdmx_indicator(
        unit_measure=["USD_PPP_PS"],
        indicator_name="hospital_expenditure_per_capita_usd_ppp",
        sha_category=["HC3", "HC.3"],
    )


def fetch_sdmx_pharma_expenditure_pct_gdp() -> pd.DataFrame:
    """
    Gasto farmacéutico como % del PIB.
    Unidad: PT_B1GQ
    Categorías fallback: HC51 / HC5_1 / HC.5.1
    Indicador: pharma_expenditure_pct_gdp
    """
    return fetch_sdmx_indicator(
        unit_measure=["PT_B1GQ"],
        indicator_name="pharma_expenditure_pct_gdp",
        sha_category=["HC51", "HC5_1", "HC.5.1"],
    )


def fetch_sdmx_hospital_expenditure_pct_gdp() -> pd.DataFrame:
    """
    Gasto hospitalario como % del PIB.
    Unidad: PT_B1GQ
    Categorías fallback: HC3 / HC.3
    Indicador: hospital_expenditure_pct_gdp
    """
    return fetch_sdmx_indicator(
        unit_measure=["PT_B1GQ"],
        indicator_name="hospital_expenditure_pct_gdp",
        sha_category=["HC3", "HC.3"],
    )


def fetch_sdmx_ptr_aw67() -> pd.DataFrame:
    """
    PTR (Participation Tax Rate) seleccionado para AW67 y total (_Z).
    Dataset: TAXBEN PTR (DF_PTRUB)
    Indicadores generados:
        - ptr_aw67   : Código AW67
        - ptr_total  : Código _Z
        - ptr_other  : Otros (fallback defensivo)
    """
    base_url = "https://sdmx.oecd.org/public/rest/data/OECD.ELS.JAI,DSD_TAXBEN_PTR@DF_PTRUB,1.0"
    query_key = "...AW67.C_C2..AW67+_Z..M2.YES.NO.NO...A"
    params = "?dimensionAtObservation=AllDimensions&format=jsondata"
    url = f"{base_url}/{query_key}{params}"

    # Filtro de países (códigos de 3 letras)
    oecd_country_codes = {k: v for k, v in COUNTRY_CODES.items() if len(v) == 3}
    code_to_name = {v: k for k, v in oecd_country_codes.items()}

    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "data" not in data or "dataSets" not in data["data"] or not data["data"]["dataSets"]:
            return pd.DataFrame()

        dataset = data["data"]["dataSets"][0]
        structure = data["data"]["structures"][0]
        obs_dims = structure["dimensions"]["observation"]

        # Buscar índices de dimensión
        ref_area_pos = time_pos = None
        ref_area_dim = time_dim = None

        # Buscar la dimensión que contiene AW67/_Z para etiquetar
        grp_pos = None
        grp_dim = None
        for i, dim in enumerate(obs_dims):
            if dim["id"] == "REF_AREA":
                ref_area_pos, ref_area_dim = i, dim
            elif dim["id"] == "TIME_PERIOD":
                time_pos, time_dim = i, dim

        # Elegir la dimensión que tenga ambos códigos si es posible
        candidates: List[tuple] = []
        for i, dim in enumerate(obs_dims):
            ids = {v.get("id") for v in dim.get("values", [])}
            if "AW67" in ids or "_Z" in ids:
                candidates.append((i, dim, ids))
        # Preferir la que contenga ambos códigos
        for i, dim, ids in candidates:
            if "AW67" in ids and "_Z" in ids:
                grp_pos, grp_dim = i, dim
                break
        if grp_pos is None and candidates:
            grp_pos, grp_dim, _ = candidates[0]

        if ref_area_pos is None or time_pos is None or "observations" not in dataset:
            return pd.DataFrame()

        rows: List[dict] = []
        for key, values in dataset["observations"].items():
            parts = key.split(":")
            try:
                ra_idx = int(parts[ref_area_pos])
                t_idx = int(parts[time_pos])
            except Exception:
                continue

            if ra_idx >= len(ref_area_dim["values"]) or t_idx >= len(time_dim["values"]):
                continue

            country_code = ref_area_dim["values"][ra_idx]["id"]
            if country_code not in code_to_name:
                continue

            gcode = None
            if grp_pos is not None:
                try:
                    g_idx = int(parts[grp_pos])
                    if g_idx < len(grp_dim["values"]):
                        gcode = grp_dim["values"][g_idx]["id"]
                except Exception:
                    pass

            val = values[0] if values and values[0] is not None else None
            if val is None:
                continue

            indicator = "ptr_aw67" if gcode == "AW67" else ("ptr_total" if gcode == "_Z" else "ptr_other")

            rows.append({
                "country": code_to_name[country_code],
                "year": int(time_dim["values"][t_idx]["id"]),
                "value": float(val),
                "indicator": indicator
            })

        return pd.DataFrame(rows)
    except Exception:
        log.error("Error al obtener datos SDMX para PTR (AW67)")
        return pd.DataFrame()


def get_health_expenditure_data() -> pd.DataFrame:
    """Ejecuta todas las funciones fetch_* y une resultados no vacíos."""
    log.info("Extracción SDMX: obteniendo indicadores de salud")
    indicators = [
        fetch_sdmx_health_expenditure(),
        fetch_sdmx_health_expenditure_per_capita(),
        fetch_sdmx_pharma_expenditure_per_capita(),
        fetch_sdmx_pharma_expenditure_pct_total(),
        fetch_sdmx_hospital_expenditure_pct_total(),
        fetch_sdmx_prevention_expenditure_pct_total(),
        fetch_sdmx_obesity_or_overweight_population(),
        fetch_sdmx_hospital_expenditure_per_capita(),
        fetch_sdmx_pharma_expenditure_pct_gdp(),
        fetch_sdmx_hospital_expenditure_pct_gdp(),
        fetch_sdmx_ptr_aw67(),
    ]

    # Filtrar DataFrames vacíos y concatenar
    valid_indicators = [df for df in indicators if not df.empty]
    if valid_indicators:
        out = pd.concat(valid_indicators, ignore_index=True)
        log.info("Extracción SDMX: %s filas (%s indicadores)", len(out), out["indicator"].nunique())
        return out
    else:
        log.warning("Extracción SDMX vacía (0 filas)")
        return pd.DataFrame()


if __name__ == "__main__":
    # Ejecución directa para pruebas manuales.
    # Muestra resumen mínimo evitando logging excesivo de datos crudos.
    df = get_health_expenditure_data()
    log.info("Total filas obtenidas: %s", len(df))
    if not df.empty:
        # Only small peeks to avoid noisy logs
        log.info("Indicadores: %s", list(df["indicator"].unique()))
        log.info("Países: %s", list(df["country"].unique()))
        log.info("Rango años: %s-%s", df["year"].min(), df["year"].max())
    else:
        log.info("No se obtuvo ningún dato")