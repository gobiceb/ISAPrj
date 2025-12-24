"""
Data Processor Module - Transforms and Aggregates Raw Data
Calculates metrics, handles missing values, and prepares data for visualizations

Author: Lead Systems Developer
Date: December 2024
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Optional, List, Dict, Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# INTERCONNECTION DATA PROCESSING
# ============================================================================

def process_interconnection_data(raw_flows: pd.DataFrame) -> pd.DataFrame:
    """
    Process and clean raw interconnection flow data
    
    Args:
        raw_flows: Raw flow data from APIs
    
    Returns:
        Cleaned and standardized DataFrame
    """
    try:
        df = raw_flows.copy()
        
        # Ensure timestamp is datetime
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Handle missing values
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        df[numeric_cols] = df[numeric_cols].fillna(method='ffill').fillna(method='bfill')
        
        # Remove duplicates
        if 'timestamp' in df.columns and 'from_country' in df.columns:
            df = df.drop_duplicates(
                subset=['timestamp', 'from_country', 'to_country'],
                keep='last'
            )
        
        # Sort by timestamp
        if 'timestamp' in df.columns:
            df = df.sort_values('timestamp')
        
        # Add derived columns
        if 'flow_mw' in df.columns and 'capacity_mw' in df.columns:
            df['utilization_pct'] = (df['flow_mw'] / df['capacity_mw']) * 100
            df['available_capacity'] = df['capacity_mw'] - df['flow_mw']
        
        # Determine flow direction
        if 'flow_mw' in df.columns:
            df['flow_direction'] = df['flow_mw'].apply(
                lambda x: 'Import' if x < 0 else 'Export' if x > 0 else 'Neutral'
            )
            df['flow_magnitude'] = df['flow_mw'].abs()
        
        logger.info(f"Processed {len(df)} flow records")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing interconnection data: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# FLOW METRICS CALCULATION
# ============================================================================

def calculate_flow_metrics(flows: pd.DataFrame,
                          window_hours: int = 24) -> pd.DataFrame:
    """
    Calculate flow statistics and metrics
    
    Args:
        flows: Processed flow data
        window_hours: Rolling window for calculations
    
    Returns:
        DataFrame with calculated metrics
    """
    try:
        df = flows.copy()
        
        if 'timestamp' not in df.columns or 'flow_mw' not in df.columns:
            return df
        
        # Set timestamp as index for rolling calculations
        df = df.set_index('timestamp')
        
        # Calculate rolling statistics
        df['flow_mean'] = df['flow_mw'].rolling(f'{window_hours}H').mean()
        df['flow_std'] = df['flow_mw'].rolling(f'{window_hours}H').std()
        df['flow_min'] = df['flow_mw'].rolling(f'{window_hours}H').min()
        df['flow_max'] = df['flow_mw'].rolling(f'{window_hours}H').max()
        
        # Calculate 7-day rolling average for anomaly detection
        df['flow_7day_avg'] = df['flow_mw'].rolling('7D').mean()
        
        # Calculate deviation from rolling average
        df['deviation_pct'] = (
            (df['flow_mw'] - df['flow_7day_avg']) / df['flow_7day_avg'] * 100
        ).fillna(0)
        
        # Detect anomalies (>20% deviation)
        df['is_anomaly'] = df['deviation_pct'].abs() > 20
        
        df = df.reset_index()
        
        logger.info(f"Calculated metrics for {df[df['is_anomaly']].shape[0]} anomalies")
        
        return df
        
    except Exception as e:
        logger.error(f"Error calculating flow metrics: {str(e)}")
        return flows

# ============================================================================
# GENERATION MIX PROCESSING
# ============================================================================

def process_generation_data(generation: pd.DataFrame) -> pd.DataFrame:
    """
    Process electricity generation data
    
    Args:
        generation: Raw generation data
    
    Returns:
        Processed generation DataFrame
    """
    try:
        df = generation.copy()
        
        # Convert to numeric
        fuel_cols = [col for col in df.columns 
                    if 'mwh' in col.lower() or 'gwh' in col.lower()]
        
        for col in fuel_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Handle missing values
        df[fuel_cols] = df[fuel_cols].fillna(0)
        
        # Calculate total if not present
        if 'total_mwh' not in df.columns:
            df['total_mwh'] = df[fuel_cols].sum(axis=1)
        
        # Calculate percentages
        renewable_cols = [col for col in fuel_cols 
                         if any(x in col.lower() for x in 
                               ['wind', 'solar', 'hydro', 'biomass'])]
        
        if renewable_cols:
            df['renewable_mwh'] = df[renewable_cols].sum(axis=1)
            df['renewable_pct'] = (df['renewable_mwh'] / df['total_mwh'] * 100).fillna(0)
        
        fossil_cols = [col for col in fuel_cols 
                      if any(x in col.lower() for x in ['coal', 'gas', 'oil'])]
        
        if fossil_cols:
            df['fossil_mwh'] = df[fossil_cols].sum(axis=1)
            df['fossil_pct'] = (df['fossil_mwh'] / df['total_mwh'] * 100).fillna(0)
        
        if 'nuclear_mwh' in fuel_cols:
            df['nuclear_pct'] = (df['nuclear_mwh'] / df['total_mwh'] * 100).fillna(0)
        
        logger.info(f"Processed generation data for {df['country'].nunique()} countries")
        
        return df
        
    except Exception as e:
        logger.error(f"Error processing generation data: {str(e)}")
        return generation

# ============================================================================
# AGGREGATION FUNCTIONS
# ============================================================================

def aggregate_by_country(flows: pd.DataFrame,
                        countries: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Aggregate flows by country
    
    Args:
        flows: Flow data
        countries: Countries to include
    
    Returns:
        Aggregated DataFrame
    """
    try:
        df = flows.copy()
        
        if 'from_country' not in df.columns:
            return df
        
        # Filter countries if specified
        if countries:
            df = df[
                (df['from_country'].isin(countries)) | 
                (df['to_country'].isin(countries))
            ]
        
        # Group by timestamp and country
        agg_dict = {
            'flow_mw': ['sum', 'mean', 'min', 'max', 'count'],
            'capacity_mw': 'sum'
        }
        
        # Exports (from_country)
        exports = df.groupby(['timestamp', 'from_country']).agg(agg_dict)
        exports.columns = ['_'.join(col).strip() for col in exports.columns.values]
        exports = exports.reset_index()
        exports.rename(columns={'from_country': 'country'}, inplace=True)
        exports['flow_type'] = 'Export'
        
        # Imports (to_country)
        imports = df.groupby(['timestamp', 'to_country']).agg(agg_dict)
        imports.columns = ['_'.join(col).strip() for col in imports.columns.values]
        imports = imports.reset_index()
        imports.rename(columns={'to_country': 'country'}, inplace=True)
        imports['flow_type'] = 'Import'
        
        result = pd.concat([exports, imports], ignore_index=True)
        
        logger.info(f"Aggregated data for {result['country'].nunique()} countries")
        
        return result
        
    except Exception as e:
        logger.error(f"Error aggregating by country: {str(e)}")
        return flows

def aggregate_by_fuel_type(generation: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate generation by fuel type globally
    
    Args:
        generation: Generation data
    
    Returns:
        Aggregated by fuel type
    """
    try:
        df = generation.copy()
        
        fuel_types = ['coal', 'gas', 'nuclear', 'hydro', 'wind', 'solar', 'biomass', 'other']
        
        agg_data = {}
        
        for fuel in fuel_types:
            fuel_col = [col for col in df.columns if fuel.lower() in col.lower()]
            if fuel_col:
                agg_data[fuel] = df[fuel_col].sum().sum()
        
        result = pd.DataFrame({
            'fuel_type': list(agg_data.keys()),
            'mwh': list(agg_data.values())
        })
        
        result['percentage'] = (result['mwh'] / result['mwh'].sum() * 100)
        result = result.sort_values('mwh', ascending=False)
        
        logger.info(f"Aggregated generation across {len(result)} fuel types")
        
        return result
        
    except Exception as e:
        logger.error(f"Error aggregating by fuel type: {str(e)}")
        return pd.DataFrame()

# ============================================================================
# TIME SERIES AGGREGATION
# ============================================================================

def aggregate_time_series(data: pd.DataFrame,
                         freq: str = 'D',
                         agg_func: str = 'mean') -> pd.DataFrame:
    """
    Aggregate time series data to different frequencies
    
    Args:
        data: Time series data
        freq: Frequency ('H', 'D', 'W', 'M', 'Y')
        agg_func: Aggregation function ('mean', 'sum', 'max', 'min')
    
    Returns:
        Aggregated DataFrame
    """
    try:
        df = data.copy()
        
        if 'timestamp' not in df.columns:
            return df
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        aggregated = df[numeric_cols].resample(freq).agg(agg_func)
        
        aggregated = aggregated.reset_index()
        
        logger.info(f"Aggregated to {freq} frequency with {len(aggregated)} records")
        
        return aggregated
        
    except Exception as e:
        logger.error(f"Error in time series aggregation: {str(e)}")
        return data

# ============================================================================
# COMPARATIVE ANALYSIS
# ============================================================================

def compare_flow_to_baseline(flows: pd.DataFrame,
                             baseline_days: int = 7) -> pd.DataFrame:
    """
    Compare current flows to baseline (rolling average)
    
    Args:
        flows: Flow data
        baseline_days: Days for baseline calculation
    
    Returns:
        Comparative analysis DataFrame
    """
    try:
        df = flows.copy()
        
        if 'timestamp' not in df.columns or 'flow_mw' not in df.columns:
            return df
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        # Calculate baseline
        df['baseline'] = df['flow_mw'].rolling(
            window=baseline_days*24, 
            center=False
        ).mean()
        
        # Calculate deviation
        df['deviation'] = df['flow_mw'] - df['baseline']
        df['deviation_pct'] = (df['deviation'] / df['baseline'] * 100).fillna(0)
        
        # Classify
        df['classification'] = df['deviation_pct'].apply(lambda x: 
            'Surge' if x > 20 else 'Drop' if x < -20 else 'Normal'
        )
        
        logger.info(f"Classified {df[df['classification'] != 'Normal'].shape[0]} anomalies")
        
        return df
        
    except Exception as e:
        logger.error(f"Error in comparative analysis: {str(e)}")
        return flows

# ============================================================================
# DATA VALIDATION
# ============================================================================

def validate_data_quality(data: pd.DataFrame) -> Dict[str, any]:
    """
    Validate data quality metrics
    
    Args:
        data: DataFrame to validate
    
    Returns:
        Dictionary with quality metrics
    """
    try:
        metrics = {
            'total_records': len(data),
            'missing_values': data.isnull().sum().to_dict(),
            'duplicate_records': data.duplicated().sum(),
            'date_range': None,
            'data_completeness': None
        }
        
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
            metrics['date_range'] = {
                'start': str(data['timestamp'].min()),
                'end': str(data['timestamp'].max())
            }
        
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        total_values = len(data) * len(numeric_cols)
        non_null_values = data[numeric_cols].notna().sum().sum()
        metrics['data_completeness'] = (non_null_values / total_values * 100) if total_values > 0 else 0
        
        logger.info(f"Data quality: {metrics['data_completeness']:.1f}% complete")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error validating data quality: {str(e)}")
        return {}

# ============================================================================
# MISSING DATA HANDLING
# ============================================================================

def handle_missing_values(data: pd.DataFrame,
                         strategy: str = 'forward_fill') -> pd.DataFrame:
    """
    Handle missing values using various strategies
    
    Args:
        data: DataFrame with missing values
        strategy: 'forward_fill', 'backward_fill', 'interpolate', 'mean'
    
    Returns:
        DataFrame with filled values
    """
    try:
        df = data.copy()
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        if strategy == 'forward_fill':
            df[numeric_cols] = df[numeric_cols].fillna(method='ffill')
        
        elif strategy == 'backward_fill':
            df[numeric_cols] = df[numeric_cols].fillna(method='bfill')
        
        elif strategy == 'interpolate':
            df[numeric_cols] = df[numeric_cols].interpolate(method='linear')
        
        elif strategy == 'mean':
            for col in numeric_cols:
                df[col].fillna(df[col].mean(), inplace=True)
        
        # Final fallback for any remaining nulls
        df[numeric_cols] = df[numeric_cols].fillna(0)
        
        logger.info(f"Handled missing values using {strategy} strategy")
        
        return df
        
    except Exception as e:
        logger.error(f"Error handling missing values: {str(e)}")
        return data
