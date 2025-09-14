import requests
import pandas as pd
from extract.constants import COUNTRY_CODES
from utils.logging import get_logger

log = get_logger(__name__)

def fetch_world_bank_indicator(indicator_code: str, indicator_name: str):
    """Descarga un indicador (todas las páginas) y normaliza columnas."""
    start_year = 1960
    end_year = 2024
    countries = ";".join(COUNTRY_CODES.values())
    
    per_page = 1000
    page = 1
    all_records = []

    while True:
        url = (
            f"https://api.worldbank.org/v2/country/{countries}/indicator/{indicator_code}"
            f"?format=json&date={start_year}:{end_year}&per_page={per_page}&page={page}"
        )
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()

        if len(json_data) < 2 or not json_data[1]:
            break

        all_records.extend(json_data[1])

        total_pages = json_data[0]["pages"]
        if page >= total_pages:
            break

        page += 1

    # Convertir a DataFrame
    df = pd.DataFrame.from_records(all_records)
    if df.empty:
        return pd.DataFrame(columns=["country", "year", "indicator", "value"])
    
    df = df[["countryiso3code", "date", "value"]]
    df.rename(columns={"countryiso3code": "country", "date": "year", "value": "value"}, inplace=True)
    df["indicator"] = indicator_name

    # Conversión de tipos
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Filtrar y mapear códigos de país
    df = df[df["country"].isin(COUNTRY_CODES.values())]
    code_to_name = {v: k for k, v in COUNTRY_CODES.items()}
    df["country"] = df["country"].map(code_to_name)

    return df[["country", "year", "indicator", "value"]].dropna(subset=["value"])

def fetch_world_bank_data():
    """Itera sobre lista de indicadores y concatena los que devuelven filas."""
    indicators = [
        # Demografía de población (existente)
        ("SP.POP.TOTL", "population"),
        ("SP.RUR.TOTL.ZS", "rural_population_pct"),
        ("SP.POP.1564.TO.ZS", "population_15_64_pct"),
        ("SP.POP.65UP.TO.ZS", "population_65_plus_pct"),
        
        # Salud y carga epidemiológica
        ("SP.DYN.LE00.IN", "life_expectancy_total"),
        ("SP.DYN.LE00.FE.IN", "life_expectancy_female"),
        ("SP.DYN.LE00.MA.IN", "life_expectancy_male"),
        ("SH.DYN.MORT", "under5_mortality_rate"),
        ("SH.STA.MALN.ZS", "malnutrition_prevalence_under5"),
        ("SH.STA.DIAB.ZS", "diabetes_prevalence_20_79"),
        ("SH.STA.OWGH.ZS", "overweight_prevalence_under5"),
        
        # Economía y gasto sanitario
        ("NY.GDP.MKTP.CD", "gdp_usd"),
        ("NY.GDP.PCAP.CD", "gdp_per_capita_usd"),
        ("SH.XPD.CHEX.GD.ZS", "health_expenditure_pct_gdp"),
        ("SH.XPD.CHEX.PC.CD", "health_expenditure_per_capita_usd"),
        
        # Factores socioeconómicos
        ("SI.POV.GINI", "gini_index"),
        ("SI.POV.LMIC.GP", "poverty_headcount_320_day"),
        ("SE.ADT.LITR.ZS", "adult_literacy_rate"),
        ("SL.UEM.TOTL.ZS", "unemployment_rate")
    ]
    log.info("Extracción World Bank: %d indicadores", len(indicators))
    
    all_data = []
    for indicator_code, indicator_name in indicators:
        try:
            df = fetch_world_bank_indicator(indicator_code, indicator_name)
            if not df.empty:
                all_data.append(df)
                log.info("Descargadas %s filas para %s", len(df), indicator_name)
            else:
                log.warning("Sin datos para %s", indicator_name)
        except Exception as e:
            log.error("Error al obtener %s: %s", indicator_name, e)
    
    if all_data:
        out = pd.concat(all_data, ignore_index=True)
        log.info("Extracción World Bank total: %s filas", len(out))
        return out
    else:
        log.warning("Extracción World Bank vacía (0 filas)")
        return pd.DataFrame(columns=["country", "year", "indicator", "value"])

if __name__ == "__main__":
    df = fetch_world_bank_data()
    # Muestra pequeña para inspección rápida sin inundar logs
    if not df.empty:
        print(df.sample(min(len(df), 10)))
