"""
Data Fetcher Module - Integrates Multiple Data Sources
Handles API calls to EIA, ENTSO-E, Electricity Maps, Ember, OWID, World Bank

Author: Lead Systems Developer
Date: December 2024
"""

import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional, Tuple, Dict, List
import json
import xml.etree.ElementTree as ET
import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# API CONFIGURATIONS (Load from environment)
# ============================================================================

class APIConfig:
    """Centralized API configuration with environment variable fallback"""
    
    # EIA API
    EIA_API_KEY = os.getenv("EIA_API_KEY", "")
    EIA_BASE_URL = "https://api.eia.gov/v2"
    
    # ENTSO-E API
    ENTSO_E_API_KEY = os.getenv("ENTSO_E_API_KEY", "")
    ENTSO_E_BASE_URL = "https://web-api.tp.entsoe.eu/api"
    
    # Electricity Maps API
    ELECTRICITY_MAPS_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY", "")
    ELECTRICITY_MAPS_BASE_URL = "https://api.electricitymaps.com/v3"
    
    # Ember API
    EMBER_BASE_URL = "https://api.ember-energy.org"
    EMBER_API_KEY = os.getenv("EMBER_API_KEY", "")
    
    # NewsAPI
    NEWSAPI_API_KEY = os.getenv("NEWSAPI_API_KEY", "")
    NEWSAPI_BASE_URL = "https://newsapi.org/v2"
    
    # World Bank (public, no key needed)
    WORLD_BANK_BASE_URL = "https://api.worldbank.org/v2"
    
    # Our World in Data (public, no key needed)
    OWID_BASE_URL = "https://raw.githubusercontent.com/owid/energy-data/master"

# ============================================================================
# EIA DATA FETCHER
# ============================================================================

def fetch_eia_data(date_range: Tuple[datetime, datetime]) -> pd.DataFrame:
    """
    Fetch sample electricity retail sales from EIA API v2.

    Uses route:
      /v2/electricity/retail-sales/data
    and filters by state + sector + time window.

    Args:
        date_range: (start_date, end_date) datetimes or dates.

    Returns:
        DataFrame with EIA data, or empty if request fails.
    """
    if not APIConfig.EIA_API_KEY:
        logger.warning("EIA_API_KEY not set; skipping EIA fetch.")
        return pd.DataFrame()

    # Convert to YYYY-MM format for monthly frequency
    start, end = date_range
    if hasattr(start, "year"):
        start_str = start.strftime("%Y-%m")
    else:
        start_str = str(start)
    if hasattr(end, "year"):
        end_str = end.strftime("%Y-%m")
    else:
        end_str = str(end)

    
    params = {
        "api_key": APIConfig.EIA_API_KEY,
        # Example filters: Colorado residential monthly data
        "facets[stateid][]": "CO",
        "facets[sectorid][]": "RES",
        "frequency": "monthly",
        "start": start_str,
        "end": end_str,
        "data[]": "value",  # v2 uses 'value' as the data field
        "offset": 0,
        "length": 5000,
    }

    try:
        logger.info(f"Fetching EIA v2 retail-sales data from {start_str} to {end_str}...")
        r = requests.get(APIConfig.EIA_BASE_URL, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()

        if "response" not in payload or "data" not in payload["response"]:
            logger.warning("Unexpected EIA v2 response structure.")
            return pd.DataFrame()

        records = payload["response"]["data"]
        df = pd.DataFrame(records)
        if df.empty:
            logger.warning("EIA v2 returned no rows for given filters.")
            return df

        # Standardize some columns
        if "period" in df.columns:
            df["timestamp"] = pd.to_datetime(df["period"])
        df["source"] = "EIA"

        logger.info(f"Fetched {len(df)} EIA v2 records.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f"EIA v2 HTTP error {e.response.status_code}: {e.response.text[:200]}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching EIA v2 data: {e}")
        return pd.DataFrame()

# ============================================================================
# ENTSO-E DATA FETCHER
# ============================================================================

def fetch_entso_data(
    date_range: Tuple[datetime, datetime],
    in_domain: str = "10YBE----------2",
    out_domain: str = "10YBE----------2",
) -> pd.DataFrame:
    """
    Fetch cross-border (or internal) flows from ENTSO-E.

    Uses:
      https://web-api.tp.entsoe.eu/api
    with documentType A44 (external trade, cross-border).
    """

    if not APIConfig.ENTSO_E_API_KEY:
        logger.warning("ENTSO_E_API_KEY not configured. Set ENTSO_E_API_KEY in .env.")
        return pd.DataFrame()

    # ENTSO-E requires UTC and 15-min timestamps in YYYYMMDDHHMM
    start_dt = datetime.combine(date_range[0], datetime.min.time()) if hasattr(date_range[0], "year") else date_range[0]
    end_dt = datetime.combine(date_range[1], datetime.max.time()) if hasattr(date_range[1], "year") else date_range[1]

    period_start = start_dt.strftime("%Y%m%d%H%M")
    period_end = end_dt.strftime("%Y%m%d%H%M")

    # TEMP: override with a known past window
    start_dt = datetime(2023, 1, 1)
    end_dt = datetime(2023, 1, 2, 23, 0)
    period_start = start_dt.strftime("%Y%m%d%H%M")
    period_end = end_dt.strftime("%Y%m%d%H%M")

    params = {
        "documentType": "A44",                    # external trade / cross-border
        "in_Domain": in_domain,
        "out_Domain": out_domain,
        "periodStart": period_start,
        "periodEnd": period_end,
        "securityToken": APIConfig.ENTSO_E_API_KEY,
    }

    try:
        logger.info(f"Fetching ENTSO-E A44 from {period_start} to {period_end}...")
        resp = requests.get(APIConfig.ENTSO_E_BASE_URL, params=params, timeout=60)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        all_flows = []

        # NOTE: real XML parsing is more involved; this is a minimal placeholder
        for timeseries in root.findall(".//TimeSeries"):
            for point in timeseries.findall(".//Point"):
                quantity = point.find("quantity")
                if quantity is None:
                    continue
                try:
                    flow_mw = float(quantity.text)
                except (TypeError, ValueError):
                    continue
                all_flows.append(
                    {
                        "timestamp": start_dt,  # you can refine this using 'position' and resolution
                        "flow_mw": flow_mw,
                        "source": "ENTSO-E",
                    }
                )

        if not all_flows:
            logger.warning("No flow data found in ENTSO-E response.")
            return pd.DataFrame()

        df = pd.DataFrame(all_flows)
        logger.info(f"Fetched {len(df)} ENTSO-E flow records.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f"ENTSO-E HTTP error {e.response.status_code}: {e.response.text[:200]}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching ENTSO-E data: {e}")
        return pd.DataFrame()
    
# ============================================================================
# ELECTRICITY MAPS DATA FETCHER
# ============================================================================

def fetch_electricity_maps_data(dt: datetime | None = None) -> pd.DataFrame:
    """
    Fetch carbon intensity from Electricity Maps using /v3/carbon-intensity/past.

    Args:
        dt: datetime (UTC) to query. If None, uses now() - 1 hour.

    Returns:
        DataFrame with one row of carbon-intensity data, or empty on error.
    """
    if not APIConfig.ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set; skipping Electricity Maps fetch.")
        return pd.DataFrame()

    if dt is None:
        dt = datetime.utcnow() - timedelta(hours=1)

    # Format as 'YYYY-MM-DD HH:MM'
    dt_str = dt.strftime("%Y-%m-%d %H:%M")

    url = APIConfig.ELECTRICITY_MAPS_BASE_URL
    headers = {"auth-token": APIConfig.ELECTRICITY_MAPS_API_KEY}
    params = {"datetime": dt_str}

    try:
        logger.info(f"Fetching Electricity Maps carbon-intensity for {dt_str}...")
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        # Wrap JSON object into a DataFrame
        df = pd.DataFrame([data])
        df["timestamp"] = pd.to_datetime(dt_str)
        df["source"] = "Electricity Maps"
        logger.info(f"Fetched Electricity Maps carbon-intensity record for {dt_str}.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f"Electricity Maps HTTP error {e.response.status_code}: {e.response.text[:200]}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching Electricity Maps data: {e}")
        return pd.DataFrame()

# ============================================================================
# EMBER DATA FETCHER
# ============================================================================

def fetch_ember_data(date_range, entity_code: str = "BRA") -> pd.DataFrame:
    """
    Fetch yearly electricity generation data from Ember API.

    Args:
        date_range: (start_date, end_date) where dates are date/datetime objects or years.
        entity_code: Ember entity code, e.g. 'BRA', 'DEU', 'WORLD'.

    Returns:
        DataFrame with Ember yearly electricity generation data.
    """
    if not APIConfig.EMBER_API_KEY:
        logger.warning("EMBER_API_KEY not set; Ember data will not be fetched.")
        return pd.DataFrame()

    # Convert date_range to year integers
    start, end = date_range
    if hasattr(start, "year"):
        start_year = start.year
    else:
        start_year = int(start)
    if hasattr(end, "year"):
        end_year = end.year
    else:
        end_year = int(end)

    url = (
        f"{APIConfig.EMBER_BASE_URL}/v1/electricity-generation/yearly"
        f"?entity_code={entity_code}"
        f"&is_aggregate_series=false"
        f"&start_date={start_year}"
        f"&end_date={end_year}"
        f"&api_key={APIConfig.EMBER_API_KEY}"
    )

    try:
        logger.info(f"Fetching Ember yearly data: {url}")
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Ember returns a JSON with a 'data' list
        records = data.get("data", data)  # support both shapes
        if not records:
            logger.warning("Ember API returned no records.")
            return pd.DataFrame()

        df = pd.DataFrame(records)
        # Normalize timestamp column if present
        if "year" in df.columns:
            df["timestamp"] = pd.to_datetime(df["year"], format="%Y")
        df["source"] = "Ember"

        logger.info(f"Fetched {len(df)} records from Ember.")
        return df

    except requests.exceptions.HTTPError as e:
        logger.error(f"Ember HTTP error {e.response.status_code}: {e.response.text[:200]}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error fetching Ember data: {e}")
        return pd.DataFrame()

# ============================================================================
# OUR WORLD IN DATA FETCHER
# ============================================================================

def fetch_owid_data_local(csv_path: str | None = None) -> pd.DataFrame:
    """
    Load OWID energy data from a local CSV file.

    Args:
        csv_path: Optional explicit path. If None, uses ./data/owid-energy-data.csv
                  relative to this file.

    Returns:
        DataFrame with OWID energy data, or empty DataFrame on error.
    """
    try:
        if csv_path is None:
            base_dir = Path(__file__).resolve().parent
            csv_path = base_dir / "data" / "owid-energy-data.csv"

        csv_path = Path(csv_path)
        if not csv_path.exists():
            logger.error(f"OWID energy CSV not found at: {csv_path}")
            return pd.DataFrame()

        df = pd.read_csv(csv_path)
        df["source"] = "Our World in Data (local)"
        logger.info(f"Loaded {len(df)} OWID energy records from {csv_path}")
        return df

    except Exception as e:
        logger.error(f"Error loading local OWID energy CSV: {e}")
        return pd.DataFrame()

# ============================================================================
# WORLD BANK DATA FETCHER (PUBLIC - no auth needed)
# ============================================================================

def fetch_world_bank_data(indicators: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Fetch energy-related data from World Bank API (PUBLIC - no auth key needed)
    
    Args:
        indicators: List of indicator codes
    
    Returns:
        DataFrame with World Bank data
    """
    try:
        # Common energy indicators from World Bank
        default_indicators = [
            'EG.ELC.ACCS.ZS',  # Access to electricity
            'EG.ELC.RNEW.ZS',  # Renewable energy
            'EG.ELC.COAL.ZS',  # Coal as % of total
            'EG.ELC.NGAS.ZS',  # Natural gas as % of total
            'EG.ELC.NUCL.ZS'   # Nuclear as % of total
        ]
        
        indicators = indicators or default_indicators
        all_data = []
        
        logger.info(f"Fetching World Bank data for {len(indicators)} indicators...")
        
        for indicator in indicators:
            try:
                params = {
                    'format': 'json',
                    'per_page': 500,
                    'date': '2010:2023'
                }
                
                response = requests.get(
                    f"{APIConfig.WORLD_BANK_BASE_URL}/country/all/indicator/{indicator}",
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                data = response.json()
                
                if len(data) > 1 and data[1]:
                    for record in data[1]:
                        if record.get('value') is not None:
                            try:
                                all_data.append({
                                    'country': record.get('country', {}).get('value'),
                                    'country_code': record.get('countryiso3code'),
                                    'indicator': indicator,
                                    'year': int(record.get('date', 0)),
                                    'value': float(record.get('value')),
                                    'source': 'World Bank'
                                })
                            except (ValueError, TypeError):
                                continue
                            
            except requests.exceptions.RequestException as e:
                logger.warning(f"Error fetching World Bank indicator {indicator}: {str(e)}")
                continue
        
        if all_data:
            df = pd.DataFrame(all_data)
            logger.info(f"✅ Fetched {len(df)} records from World Bank")
            return df
        else:
            logger.warning("No data retrieved from World Bank")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Error fetching World Bank data: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# NEWS API FETCHER
# ============================================================================

def fetch_news_data(query: str = "cross-border electricity", hours: int = 72) -> pd.DataFrame:
    """
    Fetch news articles related to electricity
    
    Args:
        query: Search query
        hours: Hours of data to fetch
    
    Returns:
        DataFrame with news articles
    """
    try:
        if not APIConfig.NEWSAPI_API_KEY:
            logger.warning("NewsAPI key not configured. Set NEWSAPI_API_KEY in .env file")
            return pd.DataFrame()
        
        from_date = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d")
        
        params = {
            'q': query,
            'from': from_date,
            'sortBy': 'publishedAt',
            'language': 'en',
            'apiKey': APIConfig.NEWSAPI_API_KEY
        }
        
        logger.info(f"Fetching news articles about '{query}'...")
        response = requests.get(
            f"{APIConfig.NEWSAPI_BASE_URL}/everything",
            params=params,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        articles = []
        
        if data.get('status') == 'ok':
            for article in data.get('articles', []):
                articles.append({
                    'published_at': pd.to_datetime(article.get('publishedAt')),
                    'title': article.get('title'),
                    'description': article.get('description'),
                    'url': article.get('url'),
                    'source': article.get('source', {}).get('name'),
                    'author': article.get('author'),
                    'source_api': 'NewsAPI'
                })
        
        if articles:
            df = pd.DataFrame(articles)
            logger.info(f"✅ Fetched {len(df)} news articles")
            return df
        else:
            logger.warning("No news articles found")
            return pd.DataFrame()
            
    except Exception as e:
        logger.error(f"❌ Error fetching news data: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# DEMO/FALLBACK DATA GENERATORS
# ============================================================================

def _generate_demo_entso_data(date_range: Tuple) -> pd.DataFrame:
    """Generate demo ENTSO-E data for testing"""
    start_dt = datetime.combine(date_range[0], datetime.min.time()) if hasattr(date_range[0], 'year') else date_range[0]
    
    routes = [
        ('Germany', 'Austria'), ('France', 'Spain'), ('Netherlands', 'Germany'),
        ('Italy', 'France'), ('Germany', 'Poland'), ('Spain', 'Portugal')
    ]
    
    data = []
    for i in range(len(date_range[0:1]) * 24):  # 24 hours
        for from_c, to_c in routes:
            data.append({
                'timestamp': start_dt + timedelta(hours=i),
                'from_country': from_c,
                'to_country': to_c,
                'flow_mw': np.random.normal(5000, 1000),
                'capacity_mw': 8000,
                'source': 'ENTSO-E (demo)'
            })
    
    logger.info(f"Generated demo ENTSO-E data with {len(data)} records")
    return pd.DataFrame(data)

def _generate_demo_ember_data(date_range: Tuple) -> pd.DataFrame:
    """Generate demo Ember data for testing"""
    start_dt = datetime.combine(date_range[0], datetime.min.time()) if hasattr(date_range[0], 'year') else date_range[0]
    
    countries = ['Germany', 'France', 'Spain', 'Italy', 'Netherlands']
    data = []
    
    for i in range(7):  # 7 days
        for country in countries:
            data.append({
                'timestamp': start_dt + timedelta(days=i),
                'country': country,
                'coal_mwh': np.random.randint(20000, 50000),
                'gas_mwh': np.random.randint(15000, 40000),
                'nuclear_mwh': np.random.randint(10000, 35000),
                'hydro_mwh': np.random.randint(5000, 20000),
                'wind_mwh': np.random.randint(10000, 30000),
                'solar_mwh': np.random.randint(5000, 25000),
                'biomass_mwh': np.random.randint(2000, 10000),
                'other_renewables_mwh': np.random.randint(1000, 5000),
                'total_mwh': 100000,
                'source': 'Ember (demo)'
            })
    
    logger.info(f"Generated demo Ember data with {len(data)} records")
    return pd.DataFrame(data)

def _generate_demo_electricity_maps_data() -> pd.DataFrame:
    """Generate demo Electricity Maps data for testing"""
    countries = ['DE', 'FR', 'AT', 'IT', 'ES', 'PL', 'US', 'CA', 'BR', 'CN']
    data = []
    
    for country in countries:
        data.append({
            'timestamp': datetime.now(),
            'country': country,
            'carbon_intensity': np.random.randint(50, 500),
            'renewable_percentage': np.random.randint(10, 80),
            'fossil_percentage': np.random.randint(10, 70),
            'nuclear_percentage': np.random.randint(0, 50),
            'flow_mw': np.random.normal(3000, 1000),
            'source': 'Electricity Maps (demo)'
        })
    
    logger.info(f"Generated demo Electricity Maps data with {len(data)} records")
    return pd.DataFrame(data)

# ============================================================================
# COMBINED DATA FETCHER
# ============================================================================

def fetch_all_data(date_range: Tuple, countries: Optional[List[str]] = None,
                  include_sources: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:
    """
    Fetch data from all available sources
    
    Args:
        date_range: Date range for historical data
        countries: List of countries
        include_sources: List of sources to include
    
    Returns:
        Dictionary with DataFrames from each source
    """
    sources = include_sources or ['eia', 'entso', 'electricity_maps', 'ember', 'owid', 'world_bank', 'news']
    
    results = {}
    
    if 'eia' in sources:
        results['eia'] = fetch_eia_data(date_range)
    if 'entso' in sources:
        results['entso'] = fetch_entso_data(date_range, countries)
    if 'electricity_maps' in sources:
        results['electricity_maps'] = fetch_electricity_maps_data(countries)
    if 'ember' in sources:
        results['ember'] = fetch_ember_data(date_range)
    if 'owid' in sources:
        results['owid'] = fetch_owid_data_local()
    if 'world_bank' in sources:
        results['world_bank'] = fetch_world_bank_data()
    if 'news' in sources:
        results['news'] = fetch_news_data()
    
    return results
