"""
Data Templates - CSV Schema for User Custom Data Entry
Use these templates to upload your organization's custom data

Author: Lead Systems Developer
Date: December 2024
"""

import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta

# ============================================================================
# CROSS-BORDER FLOWS TEMPLATE
# ============================================================================

def create_flows_template() -> pd.DataFrame:
    """Create template for cross-border electricity flows"""
    
    template = pd.DataFrame({
        'timestamp': pd.date_range(
            start=datetime.now() - timedelta(days=365),
            end=datetime.now(),
            freq='h'
        )[:24],  # Sample 24 hours
        'from_country': ['Germany'] * 6 + ['France'] * 6 + ['Austria'] * 6 + ['Spain'] * 6,
        'to_country': ['Austria'] * 6 + ['Spain'] * 6 + ['Czech Republic'] * 6 + ['Portugal'] * 6,
        'flow_mw': [5200.5, 5180.2, 5250.0, 5300.1, 5150.3, 5220.0] * 4,
        'capacity_mw': [6000.0] * 24,
        'utilization_pct': [86.7, 86.3, 87.5, 88.3, 85.8, 87.0] * 4,
        'source': ['User Import'] * 24
    })
    
    return template

# ============================================================================
# GENERATION MIX TEMPLATE
# ============================================================================

def create_generation_template() -> pd.DataFrame:
    """Create template for electricity generation mix"""
    
    template = pd.DataFrame({
        'date': pd.date_range(
            start=datetime.now() - timedelta(days=30),
            end=datetime.now(),
            freq='D'
        ),
        'country': ['Germany'] * 31,
        'coal_mwh': [50000 + i*100 for i in range(31)],
        'gas_mwh': [35000 + i*50 for i in range(31)],
        'nuclear_mwh': [0] * 31,
        'hydro_mwh': [5000 + i*20 for i in range(31)],
        'wind_mwh': [45000 + i*200 for i in range(31)],
        'solar_mwh': [15000 + i*100 for i in range(31)],
        'biomass_mwh': [8000 + i*50 for i in range(31)],
        'total_mwh': [158000 + i*500 for i in range(31)],
        'source': ['User Import'] * 31
    })
    
    return template

# ============================================================================
# POWER STATIONS TEMPLATE
# ============================================================================

def create_power_stations_template() -> pd.DataFrame:
    """Create template for power station information"""
    
    template = pd.DataFrame({
        'station_name': [
            'Coal Plant A', 'Coal Plant B', 'Gas Plant A', 'Gas Plant B',
            'Wind Farm A', 'Wind Farm B', 'Solar Array A', 'Solar Array B',
            'Nuclear Plant A', 'Hydro Plant A'
        ],
        'country': [
            'Germany', 'Germany', 'Germany', 'Austria',
            'Germany', 'Denmark', 'Spain', 'Spain',
            'France', 'Austria'
        ],
        'fuel_type': [
            'Coal', 'Coal', 'Natural Gas', 'Natural Gas',
            'Wind', 'Wind', 'Solar', 'Solar',
            'Nuclear', 'Hydro'
        ],
        'capacity_mw': [
            500, 600, 400, 300,
            150, 250, 80, 120,
            1200, 180
        ],
        'latitude': [
            51.5, 52.3, 50.1, 47.5,
            53.2, 56.1, 40.4, 39.3,
            46.2, 47.8
        ],
        'longitude': [
            10.0, 11.5, 9.5, 14.5,
            12.0, 9.5, -3.7, -8.2,
            2.2, 14.0
        ],
        'operational_year': [
            2015, 2012, 2018, 2010,
            2020, 2019, 2021, 2020,
            1985, 2000
        ],
        'efficiency_pct': [
            42, 40, 55, 53,
            35, 35, 18, 18,
            33, 85
        ],
        'status': [
            'Active', 'Active', 'Active', 'Active',
            'Active', 'Active', 'Active', 'Active',
            'Active', 'Active'
        ]
    })
    
    return template

# ============================================================================
# INTERCONNECTOR INFRASTRUCTURE TEMPLATE
# ============================================================================

def create_interconnectors_template() -> pd.DataFrame:
    """Create template for interconnector line information"""
    
    template = pd.DataFrame({
        'interconnector_id': [
            'DE-AT-001', 'DE-AT-002', 'FR-ES-001', 'FR-ES-002',
            'NL-DE-001', 'NL-DE-002', 'IT-FR-001', 'AT-CZ-001'
        ],
        'from_country': [
            'Germany', 'Germany', 'France', 'France',
            'Netherlands', 'Netherlands', 'Italy', 'Austria'
        ],
        'to_country': [
            'Austria', 'Austria', 'Spain', 'Spain',
            'Germany', 'Germany', 'France', 'Czech Republic'
        ],
        'capacity_mw': [
            6000, 5500, 5000, 4500,
            8000, 7500, 5500, 5000
        ],
        'voltage_kv': [
            380, 220, 380, 220,
            380, 220, 380, 220
        ],
        'technology': [
            'AC', 'AC', 'AC', 'AC',
            'AC', 'AC', 'AC', 'AC'
        ],
        'operational_date': [
            '2010-01-01', '2008-06-15', '2012-03-20', '2005-09-10',
            '2015-11-01', '2009-04-30', '2011-07-15', '2014-02-20'
        ],
        'owner': [
            'TSO Austria', 'TSO Austria', 'RTE', 'RTE',
             'TenneT', 'TenneT', 'TERNA', 'APG'
        ]
    })
    
    return template

# ============================================================================
# GRID MAINTENANCE TEMPLATE
# ============================================================================

def create_maintenance_template() -> pd.DataFrame:
    """Create template for grid maintenance scheduling"""
    
    template = pd.DataFrame({
        'maintenance_id': [
            'MAINT-001', 'MAINT-002', 'MAINT-003', 'MAINT-004'
        ],
        'interconnector_id': [
            'DE-AT-001', 'FR-ES-001', 'NL-DE-001', 'AT-CZ-001'
        ],
        'start_date': [
            '2024-01-15', '2024-02-01', '2024-03-10', '2024-04-05'
        ],
        'end_date': [
            '2024-01-20', '2024-02-10', '2024-03-15', '2024-04-12'
        ],
        'reduced_capacity_mw': [
            3000, 2500, 4000, 2500
        ],
        'maintenance_type': [
            'Preventive', 'Corrective', 'Preventive', 'Inspection'
        ],
        'estimated_impact_mwh': [
            15000, 12500, 20000, 12500
        ]
    })
    
    return template

# ============================================================================
# EMISSION FACTORS TEMPLATE
# ============================================================================

def create_emissions_template() -> pd.DataFrame:
    """Create template for carbon emission factors"""
    
    template = pd.DataFrame({
        'country': [
            'Germany', 'France', 'Spain', 'Austria',
            'Poland', 'Czech Republic', 'Netherlands', 'Italy',
            'Denmark', 'Sweden'
        ],
        'year': [2024] * 10,
        'coal_g_co2_per_kwh': [
            950, 0, 850, 0,
            900, 820, 0, 800, 0, 0
        ],
        'gas_g_co2_per_kwh': [
            400, 400, 400, 400,
            400, 400, 400, 400,
            400, 0
        ],
        'nuclear_g_co2_per_kwh': [
            12, 12, 12, 12,
            12, 12, 12, 12,
            12, 12
        ],
        'wind_g_co2_per_kwh': [
            10, 10, 10, 10,
            10, 10, 10, 10,
            10, 10
        ],
        'solar_g_co2_per_kwh': [
            40, 40, 40, 40,
            40, 40, 40, 40,
            40, 40
        ],
        'hydro_g_co2_per_kwh': [
            4, 4, 4, 4,
            4, 4, 4, 4,
            4, 4
        ],
        'avg_grid_intensity_g_co2_per_kwh': [
            350, 55, 280, 150,
            650, 480, 320, 300,
            45, 25
        ]
    })
    
    return template

# ============================================================================
# EXPORT FUNCTIONS
# ============================================================================

def export_all_templates(export_dir: str = "./data_templates") -> None:
    """
    Export all templates as CSV files
    
    Args:
        export_dir: Directory to export templates
    """
    export_path = Path(export_dir)
    export_path.mkdir(exist_ok=True)
    
    templates = {
        'flows_template.csv': create_flows_template(),
        'generation_template.csv': create_generation_template(),
        'power_stations_template.csv': create_power_stations_template(),
        'interconnectors_template.csv': create_interconnectors_template(),
        'maintenance_template.csv': create_maintenance_template(),
        'emissions_template.csv': create_emissions_template()
    }
    
    for filename, df in templates.items():
        filepath = export_path / filename
        df.to_csv(filepath, index=False)
        print(f"✓ Exported: {filepath}")

def create_sample_database() -> None:
    """Create sample SQLite database with all templates"""
    try:
        import sqlite3
        from datetime import datetime
        
        conn = sqlite3.connect('electricity_data.db')
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS flows (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME,
            from_country TEXT,
            to_country TEXT,
            flow_mw REAL,
            capacity_mw REAL,
            utilization_pct REAL,
            source TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS generation (
            id INTEGER PRIMARY KEY,
            date DATE,
            country TEXT,
            coal_mwh REAL,
            gas_mwh REAL,
            nuclear_mwh REAL,
            hydro_mwh REAL,
            wind_mwh REAL,
            solar_mwh REAL,
            biomass_mwh REAL,
            total_mwh REAL,
            source TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS power_stations (
            id INTEGER PRIMARY KEY,
            station_name TEXT,
            country TEXT,
            fuel_type TEXT,
            capacity_mw REAL,
            latitude REAL,
            longitude REAL,
            operational_year INTEGER,
            efficiency_pct REAL,
            status TEXT
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS interconnectors (
            id INTEGER PRIMARY KEY,
            interconnector_id TEXT UNIQUE,
            from_country TEXT,
            to_country TEXT,
            capacity_mw REAL,
            voltage_kv INTEGER,
            technology TEXT,
            operational_date DATE,
            owner TEXT
        )
        ''')
        
        # Insert sample data
        flows_df = create_flows_template()
        generation_df = create_generation_template()
        stations_df = create_power_stations_template()
        interconnectors_df = create_interconnectors_template()
        
        flows_df.to_sql('flows', conn, if_exists='append', index=False)
        generation_df.to_sql('generation', conn, if_exists='append', index=False)
        stations_df.to_sql('power_stations', conn, if_exists='append', index=False)
        interconnectors_df.to_sql('interconnectors', conn, if_exists='append', index=False)
        
        conn.commit()
        conn.close()
        
        print("✓ Created sample database: electricity_data.db")
        
    except Exception as e:
        print(f"✗ Error creating database: {str(e)}")

if __name__ == "__main__":
    print("Generating data templates...")
    export_all_templates()
    create_sample_database()
    print("\n✓ All templates generated successfully!")
