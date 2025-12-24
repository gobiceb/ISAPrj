"""
Visualizations Module - Creates Interactive Charts and Maps
Uses Plotly for interactive visualizations

Author: Lead Systems Developer
Date: December 2024
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from typing import Optional, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# INTERCONNECTION MAP
# ============================================================================

def create_interconnection_map(flows: pd.DataFrame) -> go.Figure:
    """
    Create interactive map showing cross-border interconnections
    
    Args:
        flows: Flow data with coordinates
    
    Returns:
        Plotly figure with map
    """
    try:
        # Country coordinates (sample - extend for all countries)
        country_coords = {
            'Germany': (51.1657, 10.4515),
            'France': (46.2276, 2.2137),
            'Austria': (47.5162, 14.5501),
            'Italy': (41.8719, 12.5674),
            'Spain': (40.4637, -3.7492),
            'Poland': (51.9194, 19.1451),
            'Netherlands': (52.1326, 5.2913),
            'Belgium': (50.5039, 4.4699),
            'Czech Republic': (49.8175, 15.4730),
            'Portugal': (39.3999, -8.2245),
            'Greece': (39.0742, 21.8243),
            'Sweden': (60.1282, 18.6435),
            'Norway': (60.4720, 8.4689),
            'Denmark': (56.2639, 9.5018)
        }
        
        fig = go.Figure()
        
        # Add background map
        fig.add_scattergeo(
            lon=[-10, 50],
            lat=[35, 70],
            mode='markers',
            marker=dict(size=0),
            showlegend=False
        )
        
        # Add country nodes
        lons = [country_coords[c][1] for c in country_coords.keys()]
        lats = [country_coords[c][0] for c in country_coords.keys()]
        countries = list(country_coords.keys())
        
        fig.add_scattergeo(
            lon=lons,
            lat=lats,
            text=countries,
            mode='markers+text',
            marker=dict(
                size=15,
                color='#1f77b4',
                line=dict(color='white', width=2)
            ),
            textposition='top center',
            name='Countries',
            showlegend=False
        )
        
        # Add interconnection lines (sample flows)
        if not flows.empty and 'from_country' in flows.columns:
            flows_agg = flows.groupby(['from_country', 'to_country']).agg({
                'flow_mw': 'mean',
                'capacity_mw': 'first'
            }).reset_index()
            
            for _, row in flows_agg.iterrows():
                from_country = row['from_country']
                to_country = row['to_country']
                flow = row['flow_mw']
                capacity = row['capacity_mw']
                
                if from_country in country_coords and to_country in country_coords:
                    from_lat, from_lon = country_coords[from_country]
                    to_lat, to_lon = country_coords[to_country]
                    
                    # Color based on flow direction
                    color = '#d62728' if flow > 0 else '#2ca02c'
                    width = min(max(abs(flow) / 100, 1), 5)
                    
                    fig.add_scattergeo(
                        lon=[from_lon, to_lon],
                        lat=[from_lat, to_lat],
                        mode='lines',
                        line=dict(width=width, color=color),
                        hovertext=f"{from_country}→{to_country}<br>Flow: {flow:.0f} MW",
                        showlegend=False
                    )
        
        fig.update_layout(
            title='Cross-border Electricity Interconnections',
            geo=dict(
                scope='europe',
                projection_type='mercator',
                showland=True,
                landcolor='rgb(243, 243, 243)',
                coastcolor='rgb(204, 204, 204)',
                showlakes=True,
                lakecolor='rgb(255, 255, 255)'
            ),
            height=700,
            margin=dict(l=0, r=0, t=50, b=0)
        )
        
        logger.info("Created interconnection map")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating interconnection map: {str(e)}")
        return go.Figure().add_annotation(text="Error creating map")

# ============================================================================
# GENERATION STACKED CHART
# ============================================================================

def create_generation_stacked_chart(generation: pd.DataFrame,
                                    countries: Optional[List[str]] = None) -> go.Figure:
    """
    Create stacked area chart of generation mix
    
    Args:
        generation: Generation data
        countries: Countries to display
    
    Returns:
        Plotly figure with stacked chart
    """
    try:
        df = generation.copy()
        
        # Filter countries if specified
        if countries and 'country' in df.columns:
            df = df[df['country'].isin(countries)]
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available")
        
        # Ensure timestamp column exists
        if 'timestamp' not in df.columns and 'date' in df.columns:
            df['timestamp'] = pd.to_datetime(df['date'])
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Aggregate by date and fuel type
        fuel_types = ['coal', 'gas', 'nuclear', 'hydro', 'wind', 'solar', 'biomass']
        
        agg_data = {}
        
        for fuel in fuel_types:
            fuel_cols = [col for col in df.columns if fuel.lower() in col.lower()]
            if fuel_cols:
                agg_data[fuel] = df.groupby('timestamp')[fuel_cols[0]].sum()
        
        agg_df = pd.DataFrame(agg_data).fillna(0)
        agg_df = agg_df.sort_index()
        
        fig = go.Figure()
        
        fuel_colors = {
            'coal': '#333333',
            'gas': '#ff7f0e',
            'nuclear': '#ffd700',
            'hydro': '#1f77b4',
            'wind': '#17becf',
            'solar': '#ffbb78',
            'biomass': '#2ca02c'
        }
        
        for fuel in agg_data.keys():
            fig.add_trace(go.Scatter(
                x=agg_df.index,
                y=agg_df[fuel],
                name=fuel.capitalize(),
                mode='lines',
                line=dict(width=0.5),
                fillcolor=fuel_colors.get(fuel, '#000000'),
                stackgroup='one'
            ))
        
        fig.update_layout(
            title='Electricity Generation Mix by Fuel Type',
            xaxis_title='Date',
            yaxis_title='Generation (MWh)',
            hovermode='x unified',
            height=500,
            template='plotly_white'
        )
        
        logger.info("Created generation stacked chart")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating generation chart: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")

# ============================================================================
# IMPORT/EXPORT CHART
# ============================================================================

def create_import_export_chart(eia_data: pd.DataFrame,
                               entso_data: pd.DataFrame,
                               countries: Optional[List[str]] = None) -> go.Figure:
    """
    Create chart showing import/export balance
    
    Args:
        eia_data: EIA data
        entso_data: ENTSO-E data
        countries: Countries to display
    
    Returns:
        Plotly figure with import/export chart
    """
    try:
        # Combine data sources
        all_data = []
        
        if not entso_data.empty:
            # Calculate net flows by country
            exports = entso_data.groupby('from_country')['flow_mw'].sum()
            imports = entso_data.groupby('to_country')['flow_mw'].sum() * -1
            
            for country in set(exports.index) | set(imports.index):
                all_data.append({
                    'country': country,
                    'exports': exports.get(country, 0),
                    'imports': imports.get(country, 0)
                })
        
        if not all_data:
            return go.Figure().add_annotation(text="No flow data available")
        
        df = pd.DataFrame(all_data)
        
        # Filter countries if specified
        if countries:
            df = df[df['country'].isin(countries)]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df['country'],
            y=df['exports'],
            name='Exports',
            marker_color='#d62728'
        ))
        
        fig.add_trace(go.Bar(
            x=df['country'],
            y=df['imports'],
            name='Imports',
            marker_color='#2ca02c'
        ))
        
        fig.update_layout(
            title='Import/Export Balance by Country',
            xaxis_title='Country',
            yaxis_title='Flow (MW)',
            barmode='relative',
            height=500,
            template='plotly_white'
        )
        
        logger.info("Created import/export chart")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating import/export chart: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")

# ============================================================================
# RENEWABLE CONTRIBUTION CHART
# ============================================================================

def create_renewable_contribution_chart(ember_data: pd.DataFrame,
                                       owid_data: pd.DataFrame,
                                       countries: Optional[List[str]] = None) -> go.Figure:
    """
    Create chart showing renewable energy contribution
    
    Args:
        ember_data: Ember generation data
        owid_data: Our World in Data
        countries: Countries to display
    
    Returns:
        Plotly figure with renewable chart
    """
    try:
        df = ember_data.copy()
        
        # Filter countries if specified
        if countries and 'country' in df.columns:
            df = df[df['country'].isin(countries)]
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available")
        
        # Ensure timestamp column
        if 'timestamp' not in df.columns and 'date' in df.columns:
            df['timestamp'] = pd.to_datetime(df['date'])
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp')
        
        # Get latest data per country if multiple dates
        if 'country' in df.columns:
            df = df.loc[df.groupby('country')['timestamp'].idxmax()]
        
        # Calculate renewable percentage if not present
        if 'renewable_pct' not in df.columns:
            renewable_cols = [col for col in df.columns 
                            if any(x in col.lower() for x in 
                                   ['wind', 'solar', 'hydro', 'biomass'])]
            total_col = [col for col in df.columns if 'total' in col.lower()]
            
            if renewable_cols and total_col:
                df['renewable_pct'] = (df[renewable_cols].sum(axis=1) / 
                                      df[total_col[0]] * 100)
        
        # Sort by renewable percentage
        if 'renewable_pct' in df.columns and 'country' in df.columns:
            df = df.sort_values('renewable_pct', ascending=True)
            
            fig = go.Figure(go.Bar(
                x=df['renewable_pct'],
                y=df['country'],
                orientation='h',
                marker=dict(color=df['renewable_pct'], 
                          colorscale='Greens',
                          showscale=True,
                          colorbar=dict(title="Renewable %"))
            ))
            
            fig.update_layout(
                title='Renewable Energy Contribution by Country',
                xaxis_title='Renewable Percentage (%)',
                yaxis_title='Country',
                height=600,
                template='plotly_white'
            )
            
            logger.info("Created renewable contribution chart")
            return fig
        
        return go.Figure().add_annotation(text="Insufficient data")
        
    except Exception as e:
        logger.error(f"Error creating renewable chart: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")

# ============================================================================
# FLOW TIME SERIES
# ============================================================================

def create_flow_time_series(flows: pd.DataFrame,
                           countries: Optional[List[str]] = None) -> go.Figure:
    """
    Create time series chart of cross-border flows
    
    Args:
        flows: Flow data
        countries: Countries to filter
    
    Returns:
        Plotly figure with time series
    """
    try:
        df = flows.copy()
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available")
        
        # Ensure timestamp
        if 'timestamp' not in df.columns:
            return go.Figure().add_annotation(text="No timestamp column")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        fig = go.Figure()
        
        # Plot by route if data available
        if 'from_country' in df.columns and 'to_country' in df.columns:
            routes = (df['from_country'] + '→' + df['to_country']).unique()
            
            for route in routes[:10]:  # Limit to top 10 routes
                route_data = df[
                    (df['from_country'] + '→' + df['to_country']) == route
                ].sort_values('timestamp')
                
                fig.add_trace(go.Scatter(
                    x=route_data['timestamp'],
                    y=route_data['flow_mw'],
                    name=route,
                    mode='lines'
                ))
        
        else:
            # Generic plot if no route info
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df.get('flow_mw', df.iloc[:, 1]),
                name='Flow',
                mode='lines'
            ))
        
        fig.update_layout(
            title='Cross-border Electricity Flows - Time Series',
            xaxis_title='Date',
            yaxis_title='Flow (MW)',
            hovermode='x unified',
            height=500,
            template='plotly_white'
        )
        
        logger.info("Created flow time series chart")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating time series: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")

# ============================================================================
# FUEL TYPE DISTRIBUTION
# ============================================================================

def create_fuel_type_distribution(generation: pd.DataFrame) -> go.Figure:
    """
    Create pie chart of fuel type distribution
    
    Args:
        generation: Generation data
    
    Returns:
        Plotly figure with pie chart
    """
    try:
        df = generation.copy()
        
        if df.empty:
            return go.Figure().add_annotation(text="No data available")
        
        fuel_types = ['coal', 'gas', 'nuclear', 'hydro', 'wind', 'solar', 'biomass', 'other']
        
        totals = {}
        
        for fuel in fuel_types:
            fuel_cols = [col for col in df.columns if fuel.lower() in col.lower()]
            if fuel_cols:
                totals[fuel.capitalize()] = df[fuel_cols].sum().sum()
        
        if not totals:
            return go.Figure().add_annotation(text="No generation data found")
        
        # Remove zero values
        totals = {k: v for k, v in totals.items() if v > 0}
        
        fig = go.Figure(data=[go.Pie(
            labels=list(totals.keys()),
            values=list(totals.values()),
            hole=.3
        )])
        
        fig.update_layout(
            title='Global Electricity Generation Distribution by Fuel Type',
            height=500
        )
        
        logger.info("Created fuel type distribution chart")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating fuel distribution: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")

# ============================================================================
# ANOMALY VISUALIZATION
# ============================================================================

def create_anomaly_chart(flows: pd.DataFrame) -> go.Figure:
    """
    Create chart highlighting anomalies in flows
    
    Args:
        flows: Flow data with anomaly flags
    
    Returns:
        Plotly figure with anomalies highlighted
    """
    try:
        df = flows.copy()
        
        if 'timestamp' not in df.columns or 'flow_mw' not in df.columns:
            return go.Figure().add_annotation(text="Invalid data structure")
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.sort_values('timestamp')
        
        fig = go.Figure()
        
        # Add main flow line
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['flow_mw'],
            name='Actual Flow',
            mode='lines',
            line=dict(color='blue')
        ))
        
        # Add 7-day average if available
        if 'flow_7day_avg' in df.columns:
            fig.add_trace(go.Scatter(
                x=df['timestamp'],
                y=df['flow_7day_avg'],
                name='7-Day Average',
                mode='lines',
                line=dict(color='gray', dash='dash')
            ))
        
        # Highlight anomalies
        if 'is_anomaly' in df.columns:
            anomalies = df[df['is_anomaly']]
            fig.add_trace(go.Scatter(
                x=anomalies['timestamp'],
                y=anomalies['flow_mw'],
                name='Anomalies',
                mode='markers',
                marker=dict(color='red', size=10)
            ))
        
        fig.update_layout(
            title='Flow Anomalies (Deviation >20% from 7-day average)',
            xaxis_title='Date',
            yaxis_title='Flow (MW)',
            hovermode='x unified',
            height=500,
            template='plotly_white'
        )
        
        logger.info("Created anomaly chart")
        return fig
        
    except Exception as e:
        logger.error(f"Error creating anomaly chart: {str(e)}")
        return go.Figure().add_annotation(text="Error creating chart")
