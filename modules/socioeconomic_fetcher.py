# ============================================================================
# SOCIO-ECONOMIC INDICATORS FETCHER
# ============================================================================
"""
Comprehensive socio-economic indicators from World Bank API.

Key Indicators (all free, no API key needed):
- GDP per capita (current US$): NY.GDP.PCAP.CD
- GDP growth (annual %): NY.GDP.MKTP.KD.ZG
- Population, total: SP.POP.TOTL
- Unemployment, total (% of labor force): SL.UEM.TOTL.ZS
- School enrollment, tertiary (% gross): SE.TER.ENRR
- Literacy rate, adult total (% of population 15+): SE.ADT.LITR.ZS
- Life expectancy at birth, total (years): SP.DYN.LE00.IN
- Mortality rate, under-5 (per 1,000 live births): SP.DYN.CDRT.IN
- Gini index (measure of inequality): SI.POV.GINI
- Access to electricity (% of population): EG.ELC.ACCS.ZS
- CO2 emissions (metric tons per capita): EN.ATM.CO2E.PC
- Renewable energy consumption (% of total): EG.FEC.RNEW.ZS
"""

import requests
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from typing import Optional, List, Dict

logger = logging.getLogger(__name__)

WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"

# ============================================================================
# CORE SOCIO-ECONOMIC INDICATORS (Carefully selected)
# ============================================================================

SOCIOECONOMIC_INDICATORS = {
    # Economic
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "NY.GNP.PCAP.CD": "GNI per capita (current US$)",
    
    # Demographics & Health
    "SP.POP.TOTL": "Population, total",
    "SP.DYN.LE00.IN": "Life expectancy at birth (years)",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    
    # Employment & Labor
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "SL.EMP.SELF.ZS": "Employment in agriculture (% of total)",
    
    # Education
    "SE.TER.ENRR": "School enrollment, tertiary (% gross)",
    "SE.ADT.LITR.ZS": "Literacy rate, adult total (%)",
    "SE.PRM.CMPLT.ZS": "Primary completion rate (%)",
    
    # Health
    "SP.DYN.CDRT.IN": "Mortality rate, under-5 (per 1,000 live births)",
    "SH.DYN.MORT": "Mortality rate, infant (per 1,000 live births)",
    "NY.ADJ.NNTY.PC.CD": "Adjusted net national income per capita",
    
    # Inequality & Poverty
    "SI.POV.GINI": "Gini index (measure of inequality)",
    "SI.POV.NAHC": "Poverty headcount ratio (% of population)",
    
    # Environment & Energy
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    
    # Infrastructure
    "IS.RDS.TOTL.KM": "Roads, total network (km)",
    "IT.NET.USER.ZS": "Internet users (% of population)",
}


# ============================================================================
# FETCH SINGLE INDICATOR FOR MULTIPLE COUNTRIES
# ============================================================================

def fetch_socioeconomic_indicator(
    indicator_code: str,
    countries: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    most_recent: int = 5
) -> pd.DataFrame:
    """
    Fetch a single socio-economic indicator for countries.
    
    Args:
        indicator_code: World Bank indicator code (e.g., 'NY.GDP.PCAP.CD')
        countries: List of country ISO3 codes (e.g., ['USA', 'GBR', 'DEU']).
                  If None, fetches all countries.
        years: List of years to fetch. If None, uses most_recent.
        most_recent: Number of most recent years to fetch (default: 5).
    
    Returns:
        DataFrame with columns: country, countryiso3code, year, value, indicator
    """
    url = f"{WORLD_BANK_BASE_URL}/country/all/indicator/{indicator_code}"
    
    params = {
        "format": "json",
        "per_page": 500,
        "mrnev": most_recent if not years else None,
    }
    
    if years:
        params["date"] = f"{min(years)}:{max(years)}"
    
    try:
        logger.info(f"Fetching indicator {indicator_code} for {countries or 'all countries'}...")
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        if len(data) < 2 or data[1] is None:
            logger.warning(f"No data returned for indicator {indicator_code}.")
            return pd.DataFrame()
        
        records = data[1]
        
        # Filter by country if specified
        if countries:
            countries_upper = [c.upper() for c in countries]
            records = [
                r for r in records
                if r.get("countryiso3code", "").upper() in countries_upper
            ]
        
        # Build dataframe
        rows = []
        for record in records:
            if record.get("value") is not None:
                try:
                    rows.append({
                        "country": record.get("country", {}).get("value", "Unknown"),
                        "countryiso3code": record.get("countryiso3code"),
                        "year": int(record.get("date", 0)),
                        "value": float(record.get("value")),
                        "indicator": indicator_code,
                        "indicator_name": SOCIOECONOMIC_INDICATORS.get(indicator_code, indicator_code),
                        "timestamp": datetime.utcnow(),
                    })
                except (ValueError, TypeError):
                    continue
        
        df = pd.DataFrame(rows)
        if not df.empty:
            logger.info(f"Fetched {len(df)} records for {indicator_code}.")
        return df

    except Exception as e:
        logger.error(f"Error fetching indicator {indicator_code}: {e}")
        return pd.DataFrame()


# ============================================================================
# FETCH MULTIPLE INDICATORS FOR COMPARISON
# ============================================================================

def fetch_multiple_socioeconomic_indicators(
    indicator_codes: List[str],
    countries: Optional[List[str]] = None,
    most_recent: int = 1
) -> Dict[str, pd.DataFrame]:
    """
    Fetch multiple socio-economic indicators for a set of countries.
    
    Args:
        indicator_codes: List of World Bank indicator codes.
        countries: List of country ISO3 codes.
        most_recent: Number of most recent years per indicator (default: 1 = latest year only).
    
    Returns:
        Dict with indicator_code as key, DataFrame as value.
    """
    results = {}
    
    for indicator in indicator_codes:
        df = fetch_socioeconomic_indicator(
            indicator, 
            countries=countries, 
            most_recent=most_recent
        )
        results[indicator] = df
    
    return results


# ============================================================================
# CONVENIENCE: Compare Countries on Selected Indicators
# ============================================================================

def fetch_country_profile(
    country_codes: List[str],
    indicator_codes: Optional[List[str]] = None,
    most_recent: int = 1
) -> Dict[str, pd.DataFrame]:
    """
    Fetch a comprehensive profile of countries on key socio-economic indicators.
    
    Args:
        country_codes: List of country ISO3 codes (e.g., ['USA', 'GBR', 'CHN']).
        indicator_codes: List of indicators to fetch. If None, uses predefined set.
        most_recent: Number of recent years to include (default: 1).
    
    Returns:
        Dict with profile data: each key is an indicator, value is DataFrame.
    """
    if indicator_codes is None:
        # Default key indicators for country comparison
        indicator_codes = [
            "NY.GDP.PCAP.CD",      # GDP per capita
            "NY.GDP.MKTP.KD.ZG",   # GDP growth
            "SP.POP.TOTL",         # Population
            "SP.DYN.LE00.IN",      # Life expectancy
            "SL.UEM.TOTL.ZS",      # Unemployment
            "SE.ADT.LITR.ZS",      # Literacy rate
            "EN.ATM.CO2E.PC",      # CO2 emissions per capita
            "SI.POV.GINI",         # Gini index
        ]
    
    return fetch_multiple_socioeconomic_indicators(
        indicator_codes,
        countries=country_codes,
        most_recent=most_recent
    )


# ============================================================================
# PIVOT & COMPARISON HELPERS
# ============================================================================

def pivot_indicators_by_country(data_dict: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Convert dict of (indicator -> DataFrame) into wide format for country comparison.
    
    Args:
        data_dict: Dict from fetch_multiple_socioeconomic_indicators()
    
    Returns:
        DataFrame with countries as rows, indicators as columns.
    """
    dfs_to_merge = []
    
    for indicator_code, df in data_dict.items():
        if df.empty:
            continue
        
        # Keep only most recent year per country
        pivot_df = df.sort_values("year").drop_duplicates(
            "countryiso3code", keep="last"
        )[["countryiso3code", "country", "value", "indicator_name"]]
        
        pivot_df = pivot_df.rename(
            columns={"value": indicator_code.replace(".", "_")}
        )
        pivot_df = pivot_df.drop(columns=["indicator_name"])
        
        dfs_to_merge.append(pivot_df)
    
    if not dfs_to_merge:
        return pd.DataFrame()
    
    # Merge all indicators on country
    result = dfs_to_merge[0]
    for df in dfs_to_merge[1:]:
        result = result.merge(
            df[["countryiso3code"] + [col for col in df.columns if col not in ["countryiso3code", "country"]]],
            on="countryiso3code",
            how="outer"
        )
    
    return result.set_index("country").sort_index()


def get_indicator_description(indicator_code: str) -> str:
    """Get human-readable description of an indicator."""
    return SOCIOECONOMIC_INDICATORS.get(indicator_code, indicator_code)


def list_available_indicators() -> Dict[str, str]:
    """List all available socio-economic indicators."""
    return SOCIOECONOMIC_INDICATORS.copy()
