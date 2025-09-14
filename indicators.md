# 📊 Indicadores implementados (Pipeline TFM)

Listado completo de indicadores realmente extraídos por las tres fuentes (`WHO GHO`, `World Bank`, `OECD SDMX`). Se muestran los **nombres internos** usados en el MART (columna `indicator`) y los **códigos origen**.

---

## 🌍 World Bank – Demografía, Salud básica y Economía
Fuente: `extract/world_bank.py`  
API: https://api.worldbank.org

| Indicador (interno) | Código API WB | Descripción breve |
|---------------------|---------------|-------------------|
| population | SP.POP.TOTL | Población total |
| rural_population_pct | SP.RUR.TOTL.ZS | % población rural |
| population_15_64_pct | SP.POP.1564.TO.ZS | % población 15–64 |
| population_65_plus_pct | SP.POP.65UP.TO.ZS | % población ≥65 |
| life_expectancy_total | SP.DYN.LE00.IN | Esperanza de vida (total) |
| life_expectancy_female | SP.DYN.LE00.FE.IN | Esperanza de vida mujeres |
| life_expectancy_male | SP.DYN.LE00.MA.IN | Esperanza de vida hombres |
| under5_mortality_rate | SH.DYN.MORT | Tasa mortalidad <5 años |
| malnutrition_prevalence_under5 | SH.STA.MALN.ZS | Malnutrición (% <5) |
| diabetes_prevalence_20_79 | SH.STA.DIAB.ZS | Prevalencia diabetes 20–79 |
| overweight_prevalence_under5 | SH.STA.OWGH.ZS | Sobrepeso (% <5) |
| gdp_usd | NY.GDP.MKTP.CD | PIB corriente (USD) |
| gdp_per_capita_usd | NY.GDP.PCAP.CD | PIB per cápita (USD) |
| health_expenditure_pct_gdp | SH.XPD.CHEX.GD.ZS | Gasto sanitario % PIB (WB) |
| health_expenditure_per_capita_usd | SH.XPD.CHEX.PC.CD | Gasto sanitario per cápita USD |
| gini_index | SI.POV.GINI | Índice Gini |
| poverty_headcount_320_day | SI.POV.LMIC.GP | Pobreza (≈3.20 USD/día, línea proxy) |
| adult_literacy_rate | SE.ADT.LITR.ZS | Alfabetización adulta (%) |
| unemployment_rate | SL.UEM.TOTL.ZS | Desempleo total (%) |

Notas:
- `health_expenditure_pct_gdp` y `health_expenditure_per_capita_*` también aparecen vía SDMX con distinta granularidad/unidad; la lógica de prioridad del MART escoge la fuente definida (SDMX > World Bank > WHO) si coinciden.

---

## 🏥 WHO GHO – Factores metabólicos y gasto agregado
Fuente: `extract/who_gho.py`  
API: https://ghoapi.azureedge.net/api

| Indicador (interno) | Código WHO GHO | Descripción breve |
|---------------------|----------------|-------------------|
| obesity_adults | NCD_BMI_30A | % adultos IMC ≥30 |
| overweight_adults | NCD_BMI_25A | % adultos IMC ≥25 |
| bmi_mean | NCD_BMI_MEAN | IMC medio adultos |
| diabetes_prevalence | NCD_DIABETES_PREVALENCE_AGESTD | Prevalencia diabetes (edad estandarizada) |
| raised_fpg | NCD_GLUC_04 | % glucosa elevada (≥7 mmol/L, proxy) |
| fpg_mean | NCD_GLUC_01 | Glucosa plasmática ayunas media |
| physical_inactivity | NCD_PAA | % adultos inactivos físicamente |
| che_gdp_pct | GHED_CHEGDP_SHA2011 | Gasto sanitario corriente % PIB (SHA 2011) |
| oop_share_che | GHED_OOPSCHE_SHA2011 | % gasto sanitario financiado OOP |

Notas:
- `che_gdp_pct` es comparable conceptualmente a `health_expenditure_pct_gdp` (WB / SDMX) pero con definiciones SHA 2011.
- Se conserva todo para análisis de convergencia entre fuentes.

---

## 📊 OECD SDMX – Gasto Sanitario y Otros (SHA + Living + TaxBen PTR)
Fuente: `extract/sdmx.py`  
API: https://sdmx.oecd.org/public/rest/data

Los datos de la OECD se obtienen mediante la API SDMX, que expone estadísticas normalizadas y comparables por país y año. El extractor implementa estrategias de *fallback* (unidades y categorías SHA equivalentes) para adaptarse a variaciones por versión o cobertura, y devuelve siempre un DataFrame normalizado.

| Indicador (interno) | Base / Unidad / Categoría (cuando aplica) | Descripción breve |
|---------------------|-------------------------------------------|-------------------|
| health_expenditure_pct_gdp | SHA / PT_B1GQ / _T | Gasto sanitario corriente % PIB |
| health_expenditure_per_capita_eur_ppp | SHA / EUR_PPP_PS / _T | Gasto sanitario per cápita (EUR PPP) |
| pharma_expenditure_per_capita_usd_ppp | SHA / USD_PPP_PS / HC51* | Gasto farmacéutico per cápita (USD PPP) |
| pharma_expenditure_pct_total | SHA / PT_EXP_HLTH / HC51* | % gasto sanitario asignado a farmacia |
| hospital_expenditure_pct_total | SHA / PT_EXP_HLTH / HC3* | % gasto sanitario asignado a hospitales |
| prevention_expenditure_pct_total | SHA / PT_EXP_HLTH / HC6* | % gasto sanitario asignado a prevención |
| obesity_or_overweight_population_measured | HEALTH_LVNG_BW / MSRD | % población (medido) obesidad o sobrepeso |
| obesity_or_overweight_population_self_reported | HEALTH_LVNG_BW / SR | % población (autodeclarado) obesidad o sobrepeso |
| hospital_expenditure_per_capita_usd_ppp | SHA / USD_PPP_PS / HC3* | Gasto hospitalario per cápita (USD PPP) |
| pharma_expenditure_pct_gdp | SHA / PT_B1GQ / HC51* | Gasto farmacéutico % PIB |
| hospital_expenditure_pct_gdp | SHA / PT_B1GQ / HC3* | Gasto hospitalario % PIB |
| ptr_aw67 | PTRUB / (código AW67) | Participation Tax Rate (escenario AW67) |
| ptr_total | PTRUB / (_Z) | PTR agregado total |
| ptr_other | PTRUB / (otros códigos) | Observaciones residuales / fallback |

Asteriscos (*): se aplican listas de fallback para categorías SHA equivalentes (p.ej. `HC51`, `HC5_1`, `HC.5.1`; `HC3`, `HC.3`; `HC6`, `HC.6`). El extractor intenta en orden hasta encontrar datos.

Notas:
- Algunos indicadores SDMX pueden solaparse conceptualmente con World Bank; la consolidación posterior en el MART aplica prioridad (SDMX > World Bank > WHO) para reducir duplicados al construir la tabla long.
- `ptr_other` normalmente es escaso; se conserva por robustez si aparecen códigos no previstos.

---

## 🔁 Priorización entre fuentes en el MART
Al poblar `mart.country_year_indicators` se aplica prioridad: `SDMX (OECD)` > `World Bank` > `WHO GHO`. Para cada `(country, year, indicator)` se conserva el primer valor disponible según esa jerarquía.

---

📌 Este documento refleja exactamente los indicadores presentes en los módulos de extracción actuales. Si se añaden nuevos, actualizar esta lista para mantener trazabilidad.
