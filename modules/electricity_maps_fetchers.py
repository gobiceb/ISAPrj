# ============================================================================
# ELECTRICITY MAPS - All Endpoints Fetcher
# ============================================================================
"""
Comprehensive Electricity Maps API integration.
Exposes all commonly used endpoints for the dashboard.

Endpoints:
- /v3/zones: List all available zones
- /v3/carbon-intensity/latest: Latest CO2 intensity by zone
- /v3/carbon-intensity/past: Past CO2 intensity at specific datetime
- /v3/carbon-intensity/history: Last 24h CO2 intensity time series
- /v3/power-breakdown/latest: Current power mix (fuel types + imports/exports)
- /v3/power-breakdown/past: Past power mix time series
"""

import requests
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import os

logger = logging.getLogger(__name__)

ELECTRICITY_MAPS_BASE_URL = "https://api.electricitymaps.com"
ELECTRICITY_MAPS_API_KEY = os.getenv("ELECTRICITY_MAPS_API_KEY", "")


def _get_headers() -> Dict:
    """Build auth headers for Electricity Maps."""
    return {"auth-token": ELECTRICITY_MAPS_API_KEY}


# ============================================================================
# ENDPOINT 1: GET ZONES (Available data sources and zones)
# ============================================================================

def fetch_electricity_maps_zones() -> pd.DataFrame:
    """
    Fetch list of all available zones and their access levels.
    
    Use this to discover which zones your token can access.
    
    Returns:
        DataFrame with zone, access level, and metadata.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set; cannot fetch zones.")
        return pd.DataFrame()

    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/zones"
    headers = _get_headers()

    try:
        logger.info("Fetching Electricity Maps zones list...")
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        # Expected response: list of zone objects with zone, access fields
        if isinstance(data, list):
            df = pd.DataFrame(data)
        else:
            df = pd.DataFrame([data])
        
        df["timestamp"] = datetime.utcnow()
        df["source"] = "Electricity Maps - Zones"
        logger.info(f"Fetched {len(df)} zones from Electricity Maps.")
        return df

    except Exception as e:
        logger.error(f"Error fetching Electricity Maps zones: {e}")
        return pd.DataFrame()


# ============================================================================
# ENDPOINT 2: CARBON INTENSITY - LATEST
# ============================================================================

def fetch_electricity_maps_carbon_latest(zone: str = None, lon: float = None, lat: float = None) -> pd.DataFrame:
    """
    Fetch latest carbon intensity of electricity consumed.
    
    Args:
        zone: Zone code (e.g., 'DE', 'FR'). If None, uses lon/lat.
        lon: Longitude (if zone not provided).
        lat: Latitude (if zone not provided).
    
    Returns:
        DataFrame with latest carbon intensity.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set.")
        return pd.DataFrame()

    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/carbon-intensity/latest"
    headers = _get_headers()
    params = {}
    
    if zone:
        params["zone"] = zone
    elif lon is not None and lat is not None:
        params["lon"] = lon
        params["lat"] = lat
    else:
        logger.warning("Either zone or (lon, lat) must be provided.")
        return pd.DataFrame()

    try:
        logger.info(f"Fetching latest carbon intensity for {zone or f'({lon},{lat})'} ...")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        df = pd.DataFrame([data])
        df["timestamp"] = pd.to_datetime(df.get("datetime", datetime.utcnow()))
        df["source"] = "Electricity Maps - Carbon Intensity Latest"
        logger.info(f"Fetched latest carbon intensity.")
        return df

    except Exception as e:
        logger.error(f"Error fetching latest carbon intensity: {e}")
        return pd.DataFrame()


# ============================================================================
# ENDPOINT 3: CARBON INTENSITY - PAST (at specific datetime)
# ============================================================================

def fetch_electricity_maps_carbon_past(
    dt: datetime = None, 
    zone: str = None, 
    lon: float = None, 
    lat: float = None
) -> pd.DataFrame:
    """
    Fetch carbon intensity at a specific past datetime.
    
    Args:
        dt: Datetime to query (UTC). If None, uses 1 hour ago.
        zone: Zone code (e.g., 'DE', 'FR').
        lon: Longitude (if zone not provided).
        lat: Latitude (if zone not provided).
    
    Returns:
        DataFrame with past carbon intensity.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set.")
        return pd.DataFrame()

    if dt is None:
        dt = datetime.utcnow() - timedelta(hours=1)

    dt_str = dt.strftime("%Y-%m-%d %H:%M")
    
    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/carbon-intensity/past"
    headers = _get_headers()
    params = {"datetime": dt_str}
    
    if zone:
        params["zone"] = zone
    elif lon is not None and lat is not None:
        params["lon"] = lon
        params["lat"] = lat
    else:
        logger.warning("Either zone or (lon, lat) must be provided.")
        return pd.DataFrame()

    try:
        logger.info(f"Fetching past carbon intensity for {dt_str}...")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        df = pd.DataFrame([data])
        df["timestamp"] = pd.to_datetime(dt_str)
        df["source"] = "Electricity Maps - Carbon Intensity Past"
        logger.info(f"Fetched past carbon intensity for {dt_str}.")
        return df

    except Exception as e:
        logger.error(f"Error fetching past carbon intensity: {e}")
        return pd.DataFrame()


# ============================================================================
# ENDPOINT 4: CARBON INTENSITY - HISTORY (Last 24 hours)
# ============================================================================

def fetch_electricity_maps_carbon_history(
    zone: str = None, 
    lon: float = None, 
    lat: float = None,
    hours: int = 24
) -> pd.DataFrame:
    """
    Fetch carbon intensity history (last 24h by default).
    
    Args:
        zone: Zone code (e.g., 'DE', 'FR').
        lon: Longitude (if zone not provided).
        lat: Latitude (if zone not provided).
        hours: How many hours back to fetch (max typically 24-48).
    
    Returns:
        DataFrame with hourly carbon intensity time series.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set.")
        return pd.DataFrame()

    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/carbon-intensity/history"
    headers = _get_headers()
    params = {"hours": hours}
    
    if zone:
        params["zone"] = zone
    elif lon is not None and lat is not None:
        params["lon"] = lon
        params["lat"] = lat
    else:
        logger.warning("Either zone or (lon, lat) must be provided.")
        return pd.DataFrame()

    try:
        logger.info(f"Fetching {hours}h carbon intensity history for {zone or f'({lon},{lat})'} ...")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        # Expected: { "history": [...] } or direct list
        if isinstance(data, dict) and "history" in data:
            records = data["history"]
        elif isinstance(data, list):
            records = data
        else:
            records = [data]
        
        df = pd.DataFrame(records)
        if "datetime" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetime"])
        else:
            df["timestamp"] = datetime.utcnow()
        
        df["source"] = "Electricity Maps - Carbon Intensity History"
        logger.info(f"Fetched {len(df)} records of carbon intensity history.")
        return df

    except Exception as e:
        logger.error(f"Error fetching carbon intensity history: {e}")
        return pd.DataFrame()


# ============================================================================
# ENDPOINT 5: POWER BREAKDOWN - LATEST (current fuel mix + imports/exports)
# ============================================================================

def fetch_electricity_maps_power_latest(
    zone: str = None, 
    lon: float = None, 
    lat: float = None
) -> pd.DataFrame:
    """
    Fetch latest power generation breakdown by fuel type and imports/exports.
    
    Returns columns like:
    - coal, gas, hydro, nuclear, wind, solar, biomass, other, etc. (in MW or %)
    - powerImportTotal, powerExportTotal (in MW)
    - renewablePercentage, fossilPercentage, nuclearPercentage
    - carbonIntensity
    
    Args:
        zone: Zone code (e.g., 'DE', 'FR').
        lon: Longitude (if zone not provided).
        lat: Latitude (if zone not provided).
    
    Returns:
        DataFrame with latest power breakdown.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set.")
        return pd.DataFrame()

    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/power-breakdown/latest"
    headers = _get_headers()
    params = {}
    
    if zone:
        params["zone"] = zone
    elif lon is not None and lat is not None:
        params["lon"] = lon
        params["lat"] = lat
    else:
        logger.warning("Either zone or (lon, lat) must be provided.")
        return pd.DataFrame()

    try:
        logger.info(f"Fetching latest power breakdown for {zone or f'({lon},{lat})'} ...")
        r = requests.get(url, headers=headers, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        df = pd.DataFrame([data])
        df["timestamp"] = pd.to_datetime(df.get("datetime", datetime.utcnow()))
        df["source"] = "Electricity Maps - Power Breakdown Latest"
        logger.info(f"Fetched latest power breakdown.")
        return df

    except Exception as e:
        logger.error(f"Error fetching latest power breakdown: {e}")
        return pd.DataFrame()


# ============================================================================
# ENDPOINT 6: POWER BREAKDOWN - PAST (time series)
# ============================================================================

def fetch_electricity_maps_power_past(
    zone: str = None,
    lon: float = None,
    lat: float = None,
    start_dt: datetime = None,
    end_dt: datetime = None
) -> pd.DataFrame:
    """
    Fetch power breakdown time series over a date range.
    
    Note: API may limit to ~24 hours or ~10 days depending on plan.
    
    Args:
        zone: Zone code (e.g., 'DE', 'FR').
        lon: Longitude (if zone not provided).
        lat: Latitude (if zone not provided).
        start_dt: Start datetime (UTC). If None, uses 24h ago.
        end_dt: End datetime (UTC). If None, uses now.
    
    Returns:
        DataFrame with power breakdown time series.
    """
    if not ELECTRICITY_MAPS_API_KEY:
        logger.warning("ELECTRICITY_MAPS_API_KEY not set.")
        return pd.DataFrame()

    if end_dt is None:
        end_dt = datetime.utcnow()
    if start_dt is None:
        start_dt = end_dt - timedelta(hours=24)

    start_str = start_dt.strftime("%Y-%m-%d %H:%M")
    end_str = end_dt.strftime("%Y-%m-%d %H:%M")
    
    url = f"{ELECTRICITY_MAPS_BASE_URL}/v3/power-breakdown/past"
    headers = _get_headers()
    params = {
        "start": start_str,
        "end": end_str,
    }
    
    if zone:
        params["zone"] = zone
    elif lon is not None and lat is not None:
        params["lon"] = lon
        params["lat"] = lat
    else:
        logger.warning("Either zone or (lon, lat) must be provided.")
        return pd.DataFrame()

    try:
        logger.info(f"Fetching power breakdown from {start_str} to {end_str}...")
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        
        # Expected: { "data": [...] } or direct list
        if isinstance(data, dict) and "data" in data:
            records = data["data"]
        elif isinstance(data, list):
            records = data
        else:
            records = [data]
        
        df = pd.DataFrame(records)
        if "datetime" in df.columns:
            df["timestamp"] = pd.to_datetime(df["datetime"])
        else:
            df["timestamp"] = datetime.utcnow()
        
        df["source"] = "Electricity Maps - Power Breakdown Past"
        logger.info(f"Fetched {len(df)} records of power breakdown history.")
        return df

    except Exception as e:
        logger.error(f"Error fetching power breakdown history: {e}")
        return pd.DataFrame()


# ============================================================================
# CONVENIENCE: Fetch all endpoints for a single zone
# ============================================================================

def fetch_electricity_maps_full_profile(zone: str) -> Dict[str, pd.DataFrame]:
    """
    Convenience function to fetch all available Electricity Maps data for a zone.
    
    Args:
        zone: Zone code (e.g., 'DE', 'FR').
    
    Returns:
        Dict with keys: latest_carbon, latest_power, carbon_history, power_history.
    """
    profile = {}
    
    profile["latest_carbon"] = fetch_electricity_maps_carbon_latest(zone=zone)
    profile["latest_power"] = fetch_electricity_maps_power_latest(zone=zone)
    profile["carbon_history"] = fetch_electricity_maps_carbon_history(zone=zone, hours=24)
    profile["power_history"] = fetch_electricity_maps_power_past(zone=zone)
    
    return profile
