"""
International Cross-border Electricity Interconnection MIS Dashboard
Main Streamlit Application Entry Point

Cleaned version: no synthetic metrics/tables; only derived from real data
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.graph_objects as go
import plotly.express as px
from pathlib import Path
import sys

# Add modules to path
sys.path.append(str(Path(__file__).parent))

from modules.data_fetcher import (
    fetch_eia_data,
    fetch_electricity_maps_data,
    fetch_entso_data,
    fetch_ember_data,
    fetch_owid_data_local,
    fetch_world_bank_data,
    fetch_news_data,
)
from modules.data_processor import (
    process_interconnection_data,
    calculate_flow_metrics,
    aggregate_by_country,
    aggregate_by_fuel_type,
)
from modules.visualizations import (
    create_interconnection_map,
    create_generation_stacked_chart,
    create_import_export_chart,
    create_renewable_contribution_chart,
    create_flow_time_series,
    create_fuel_type_distribution,
)
from modules.newsletter_engine import (
    generate_newsletter,
    detect_surge_alerts,
    export_newsletter_pdf,
)
from modules.cache_manager import CacheManager

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Cross-border Electricity Interconnection MIS",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Minimal CSS hook (no synthetic content)
st.markdown(
    """
    <style>
    .main {
        padding: 2rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================================
# SIDEBAR CONFIGURATION
# ============================================================================

with st.sidebar:
    st.title("⚡ Navigation")

    page = st.radio(
        "Select Dashboard View",
        options=[
            "🏠 Home",
            "📊 Historical Data Analysis",
            "🔴 Live Data Feed",
            "🗺️ Interconnection Map",
            "📈 Analytics & Insights",
            "🌍 Socio-Economic Indicators",
            "📰 Newsletter Engine",
            "⚙️ Data Management",
            "📋 API Configuration",
        ],
    )

    st.divider()

    # Data refresh control
    st.subheader("Cache & Refresh")
    refresh_cache = st.checkbox("Force Refresh Data", value=False)

    cache_interval = st.selectbox(
        "Cache Duration",
        options=["5 minutes", "30 minutes", "1 hour", "24 hours"],
        index=1,
    )

    st.divider()

    # Date range selector (for historical data)
    st.subheader("Time Range Filter")
    date_range = st.date_input(
        "Select Date Range",
        value=(datetime.now() - timedelta(days=365), datetime.now()),
        max_value=datetime.now(),
    )

    st.divider()

    # Country/Region filter
    st.subheader("Geographic Filter")
    all_countries = st.checkbox("All Countries", value=True)

    if not all_countries:
        selected_countries = st.multiselect(
            "Select Countries",
            options=[
                "Germany",
                "France",
                "Spain",
                "Italy",
                "Netherlands",
                "Belgium",
                "Poland",
                "Austria",
                "Czech Republic",
                "Portugal",
                "USA",
                "Canada",
                "Mexico",
                "Brazil",
                "Chile",
                "China",
                "India",
                "Japan",
                "South Korea",
                "Australia",
            ],
            default=["Germany", "France"],
        )
    else:
        selected_countries = None

# ============================================================================
# PAGE FUNCTIONS
# ============================================================================


def home_page(date_range, selected_countries):
    """Home Dashboard Overview"""
    st.title("⚡ International Cross-border Electricity Interconnection")

    st.markdown(
        """
        This MIS Dashboard monitors and analyzes cross-border electricity
        interconnections.
        """
    )

    st.divider()

    cache_mgr = CacheManager(cache_ttl_minutes=30)

    with st.spinner("Loading summary data..."):
        try:
            entso_data = cache_mgr.get_or_fetch(
                "home_entso",
                lambda: fetch_entso_data(date_range),
                force_refresh=refresh_cache,
            )
            ember_data = cache_mgr.get_or_fetch(
                "home_ember",
                lambda: fetch_ember_data(date_range),
                force_refresh=refresh_cache,
            )
            owid_data = cache_mgr.get_or_fetch(
                "home_owid", lambda: fetch_owid_data_local(), force_refresh=refresh_cache
            )
        except Exception as e:
            st.error(f"Error loading home summary data: {e}")
            entso_data, ember_data, owid_data = None, None, None

    # KPI row based only on real data if available
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if entso_data is not None and not entso_data.empty:
            # approximate number of unique routes
            if {"from_country", "to_country"}.issubset(entso_data.columns):
                routes = (
                    entso_data["from_country"] + "→" + entso_data["to_country"]
                ).nunique()
                st.metric("Interconnector Routes", f"{routes}")
            else:
                st.metric("Interconnector Routes", "N/A")
        else:
            st.metric("Interconnector Routes", "N/A")

    with col2:
        if ember_data is not None and "country" in ember_data.columns:
            countries = ember_data["country"].nunique()
            st.metric("Countries in Ember Data", f"{countries}")
        else:
            st.metric("Countries in Ember Data", "N/A")

    with col3:
        if entso_data is not None and "flow_mw" in entso_data.columns:
            avg_flow = entso_data["flow_mw"].abs().mean()
            if pd.notna(avg_flow):
                st.metric("Avg Cross-border Flow (MW)", f"{avg_flow:,.0f}")
            else:
                st.metric("Avg Cross-border Flow (MW)", "N/A")
        else:
            st.metric("Avg Cross-border Flow (MW)", "N/A")

    with col4:
        if ember_data is not None and "total_mwh" in ember_data.columns:
            total_gen = ember_data["total_mwh"].sum()
            st.metric("Total Generation (MWh)", f"{total_gen:,.0f}")
        else:
            st.metric("Total Generation (MWh)", "N/A")

    st.divider()

    st.subheader("Quick Statistics (Derived)")

    tab1, tab2 = st.tabs(["Global Overview", "Recent Alerts"])

    with tab1:
        if ember_data is not None and not ember_data.empty:
            st.write("Generation data snapshot (top 10 rows):")
            st.dataframe(ember_data.head(10), use_container_width=True)
        else:
            st.info("No generation data available for the selected period.")

    with tab2:
        if entso_data is not None and not entso_data.empty:
            # Use newsletter_engine logic to detect alerts
            processed = entso_data.copy()
            alerts = detect_surge_alerts(processed, deviation_threshold=20)
            if alerts:
                st.write("Surge alerts derived from ENTSO-E data (last 72 hours):")
                alerts_df = pd.DataFrame(alerts)
                st.dataframe(alerts_df, use_container_width=True)
            else:
                st.info("No surge alerts detected in the available data.")
        else:
            st.info("No flow data available to detect alerts.")

    st.divider()

    st.subheader("Configured Data Sources")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("**Official APIs**")
        st.markdown("- EIA\n- ENTSO-E\n- Electricity Maps\n- Ember")
    with col2:
        st.markdown("**Open Data**")
        st.markdown("- Our World in Data\n- World Bank")
    with col3:
        st.markdown("**Custom Data**")
        st.markdown("- User Uploads\n- News Feed Integration\n- Alert System")


def historical_data_page(date_range, selected_countries, refresh_cache):
    """Historical Data Analysis Page"""
    st.title("📊 Historical Data Analysis")

    st.info(
        "Historical analysis of cross-border flows, generation mix, and emissions "
        "derived from real data sources."
    )

    cache_mgr = CacheManager(cache_ttl_minutes=30)

    # Data fetching
    with st.spinner("Loading historical data..."):
        try:
            eia_data = cache_mgr.get_or_fetch(
                "eia_data",
                lambda: fetch_eia_data(date_range),
                force_refresh=refresh_cache,
            )
            entso_data = cache_mgr.get_or_fetch(
                "entso_data",
                lambda: fetch_entso_data(date_range),
                force_refresh=refresh_cache,
            )
            ember_data = cache_mgr.get_or_fetch(
                "ember_data",
                lambda: fetch_ember_data(date_range),
                force_refresh=refresh_cache,
            )
            owid_data = cache_mgr.get_or_fetch(
                "owid_data",
                lambda: fetch_owid_data_local(),
                force_refresh=refresh_cache,
            )
            st.success("Data loaded successfully")
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return

    # Tabs for different analyses
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        [
            "Flows Timeline",
            "Generation Mix",
            "Import/Export",
            "Renewable Contribution",
            "Data Tables",
        ]
    )

    with tab1:
        st.subheader("Cross-border Electricity Flows - Time Series")
        if entso_data is not None and not entso_data.empty:
            fig = create_flow_time_series(entso_data, selected_countries)
            st.plotly_chart(fig, use_container_width=True, key="hist_flows_ts")
        else:
            st.info("No ENTSO-E flow data available for the selected period.")

    with tab2:
        st.subheader("Electricity Generation Mix by Country")
        if ember_data is not None and not ember_data.empty:
            fig = create_generation_stacked_chart(ember_data, selected_countries)
            st.plotly_chart(fig, use_container_width=True, key="hist_gen_mix")
        else:
            st.info("No Ember generation data available for the selected period.")

    with tab3:
        st.subheader("Import/Export Balance")
        if (
            eia_data is not None
            and not eia_data.empty
            and entso_data is not None
            and not entso_data.empty
        ):
            fig = create_import_export_chart(eia_data, entso_data, selected_countries)
            st.plotly_chart(fig, use_container_width=True, key="hist_imp_exp")
        else:
            st.info("Insufficient EIA/ENTSO-E data for import/export analysis.")

    with tab4:
        st.subheader("Renewable Energy Contribution")
        if ember_data is not None and not ember_data.empty:
            fig = create_renewable_contribution_chart(
                ember_data, owid_data, selected_countries
            )
            st.plotly_chart(fig, use_container_width=True, key="hist_renewables")
        else:
            st.info("No generation data to compute renewable contribution.")

    with tab5:
        st.subheader("Historical Data Tables")
        subtab1, subtab2, subtab3 = st.tabs(["Flows", "Generation", "Emissions"])

        with subtab1:
            st.write("Cross-border Electricity Flows")
            if entso_data is not None and not entso_data.empty:
                st.dataframe(entso_data.head(200), use_container_width=True)
                csv = entso_data.to_csv(index=False)
                st.download_button(
                    "Download Flows Data (CSV)", csv, "flows_data.csv"
                )
            else:
                st.info("No ENTSO-E flow data available.")

        with subtab2:
            st.write("Generation Mix by Country")
            if ember_data is not None and not ember_data.empty:
                st.dataframe(ember_data.head(200), use_container_width=True)
                csv = ember_data.to_csv(index=False)
                st.download_button(
                    "Download Generation Data (CSV)", csv, "generation_data.csv"
                )
            else:
                st.info("No Ember generation data available.")

        with subtab3:
            st.write("Carbon Emissions and Intensity")
            if owid_data is not None and not owid_data.empty:
                st.dataframe(owid_data.head(200), use_container_width=True)
                csv = owid_data.to_csv(index=False)
                st.download_button(
                    "Download Emissions Data (CSV)", csv, "emissions_data.csv"
                )
            else:
                st.info("No OWID emissions data available.")


def live_data_page(selected_countries, refresh_cache):
    """Live Data Feed Page – Real-time Electricity Maps data"""
    st.title("🔴 Live Data Feed")
    st.warning("Live data refreshes every 5 minutes from Electricity Maps API")

    from modules.electricity_maps_fetchers import (
        fetch_electricity_maps_zones,
        fetch_electricity_maps_carbon_latest,
        fetch_electricity_maps_power_latest,
    )

    cache_mgr = CacheManager(cache_ttl_minutes=5)

    if st.button("🔄 Refresh Now"):
        cache_mgr.clear_all()
        st.rerun()

    # First, get available zones
    with st.spinner("Loading available zones..."):
        zones_data = cache_mgr.get_or_fetch(
            "em_zones",
            lambda: fetch_electricity_maps_zones(),
            force_refresh=refresh_cache,
        )

    if zones_data is None or zones_data.empty:
        st.error("Could not fetch zones from Electricity Maps. Check API key.")
        return

    # Let user select zones to monitor
    available_zones = zones_data["zone"].tolist() if "zone" in zones_data.columns else []
    if not available_zones:
        st.error("No zones available. Check your Electricity Maps token access.")
        return

    selected_zones = st.multiselect(
        "Select zones to monitor",
        options=available_zones,
        default=available_zones[:5] if len(available_zones) > 5 else available_zones,
    )

    if not selected_zones:
        st.info("No zones selected. Pick one or more above.")
        return

    st.divider()
    st.subheader("1. Carbon Intensity (Latest)")

    # Fetch latest carbon intensity for selected zones
    carbon_data = []
    for zone in selected_zones:
        with st.spinner(f"Loading carbon intensity for {zone}..."):
            zone_carbon = cache_mgr.get_or_fetch(
                f"em_carbon_latest_{zone}",
                lambda z=zone: fetch_electricity_maps_carbon_latest(zone=z),
                force_refresh=refresh_cache,
            )
            if zone_carbon is not None and not zone_carbon.empty:
                carbon_data.append(zone_carbon)

    if carbon_data:
        carbon_df = pd.concat(carbon_data, ignore_index=True)
        st.dataframe(carbon_df[["zone", "carbonIntensity", "datetime"]], width="stretch")

        # Visualization: carbon intensity by zone
        if "carbonIntensity" in carbon_df.columns and "zone" in carbon_df.columns:
            fig = px.bar(
                carbon_df,
                x="zone",
                y="carbonIntensity",
                title="Carbon Intensity (gCO₂/kWh) by Zone",
                labels={"carbonIntensity": "gCO₂/kWh", "zone": "Zone"},
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No carbon intensity data available for selected zones.")

    st.divider()
    st.subheader("2. Power Breakdown (Latest)")

    # Fetch latest power mix for selected zones
    power_data = []
    for zone in selected_zones:
        with st.spinner(f"Loading power breakdown for {zone}..."):
            zone_power = cache_mgr.get_or_fetch(
                f"em_power_latest_{zone}",
                lambda z=zone: fetch_electricity_maps_power_latest(zone=z),
                force_refresh=refresh_cache,
            )
            if zone_power is not None and not zone_power.empty:
                power_data.append(zone_power)

    if power_data:
        power_df = pd.concat(power_data, ignore_index=True)
        st.dataframe(
            power_df[[
                "zone",
                "renewablePercentage",
                "fossilPercentage",
                "nuclearPercentage",
                "powerImportTotal",
                "powerExportTotal"
            ]].fillna("N/A"),
            width="stretch"
        )

        # Visualization: renewable percentage by zone
        if "renewablePercentage" in power_df.columns and "zone" in power_df.columns:
            fig = px.bar(
                power_df,
                x="zone",
                y="renewablePercentage",
                title="Renewable Energy Percentage by Zone",
                labels={"renewablePercentage": "Renewable %", "zone": "Zone"},
            )
            st.plotly_chart(fig, use_container_width=True)

        # KPIs
        st.divider()
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_renewable = power_df["renewablePercentage"].mean()
            st.metric("Avg Renewable %", f"{avg_renewable:.1f}%")
        
        with col2:
            avg_carbon = power_df["carbonIntensity"].mean()
            st.metric("Avg Carbon Intensity", f"{avg_carbon:.0f} gCO₂/kWh")
        
        with col3:
            total_imports = power_df["powerImportTotal"].sum()
            st.metric("Total Imports (MW)", f"{total_imports:,.0f}")
        
        with col4:
            total_exports = power_df["powerExportTotal"].sum()
            st.metric("Total Exports (MW)", f"{total_exports:,.0f}")
    else:
        st.info("No power breakdown data available for selected zones.")


def map_page(date_range, selected_countries, refresh_cache):
    """Interconnection Map Page – ENTSO-E flows + Electricity Maps color coding"""
    st.title("🗺️ Cross-border Interconnection Map")

    from modules.electricity_maps_fetchers import (
        fetch_electricity_maps_zones,
        fetch_electricity_maps_power_latest,
    )

    st.info(
        "Map uses ENTSO-E flow data and Electricity Maps power breakdown to "
        "color-code zones by carbon intensity or renewable percentage."
    )

    cache_mgr = CacheManager(cache_ttl_minutes=30)

    # Load ENTSO-E flows
    with st.spinner("Loading ENTSO-E interconnector data..."):
        try:
            entso_data = cache_mgr.get_or_fetch(
                "entso_map_data",
                lambda: fetch_entso_data(date_range),
                force_refresh=refresh_cache,
            )
            st.success("ENTSO-E data loaded")
        except Exception as e:
            st.error(f"Error loading ENTSO-E data: {str(e)}")
            return

    # Load Electricity Maps zone data for coloring
    with st.spinner("Loading Electricity Maps zone colors..."):
        zones_data = cache_mgr.get_or_fetch(
            "em_zones_map",
            lambda: fetch_electricity_maps_zones(),
            force_refresh=refresh_cache,
        )

    # Color scheme selector
    color_by = st.radio(
        "Color zones by:",
        options=["Carbon Intensity", "Renewable Percentage", "None"],
        horizontal=True,
    )

    zone_colors = {}
    if color_by != "None" and zones_data is not None and not zones_data.empty:
        available_zones = zones_data["zone"].tolist() if "zone" in zones_data.columns else []

        if available_zones:
            with st.spinner("Fetching zone metrics for map coloring..."):
                for zone in available_zones[:10]:  # Limit to first 10 to avoid API spam
                    zone_power = cache_mgr.get_or_fetch(
                        f"em_map_power_{zone}",
                        lambda z=zone: fetch_electricity_maps_power_latest(zone=z),
                        force_refresh=refresh_cache,
                    )

                    if zone_power is not None and not zone_power.empty:
                        if color_by == "Carbon Intensity" and "carbonIntensity" in zone_power.columns:
                            zone_colors[zone] = float(zone_power["carbonIntensity"].iloc[0])
                        elif color_by == "Renewable Percentage" and "renewablePercentage" in zone_power.columns:
                            zone_colors[zone] = float(zone_power["renewablePercentage"].iloc[0])

    if entso_data is None or entso_data.empty:
        st.info("No ENTSO-E flow data available for the selected period.")
        st.info(f"Zone colors available: {len(zone_colors)} zones fetched from Electricity Maps.")
        return

    # Create map visualization (reuse existing function)
    fig = create_interconnection_map(entso_data)
    st.plotly_chart(fig, width="stretch", key="map_interconn")

    st.divider()

    # Show zone metrics table
    if zone_colors:
        st.subheader(f"Zone Metrics (Colored by: {color_by})")
        zone_metrics_df = pd.DataFrame(
            [{"zone": z, "value": v} for z, v in zone_colors.items()]
        )
        st.dataframe(zone_metrics_df, width="stretch")

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
**Interconnector Lines Legend**
- 🔵 Blue Arrow: Import (receiving power)
- 🔴 Red Arrow: Export (sending power)
- Line Width: Interconnector capacity
""")
    with col2:
        st.markdown(f"""
**Zone Coloring (by {color_by})**
- Green: Low carbon intensity / High renewable %
- Orange: Medium
- Red: High carbon intensity / Low renewable %
""")

def analytics_page(date_range, selected_countries, refresh_cache):
    """Analytics & Insights Page – Historical trends from Electricity Maps"""
    st.title("📈 Analytics & Insights")

    from modules.electricity_maps_fetchers import (
        fetch_electricity_maps_carbon_history,
        fetch_electricity_maps_power_past,
    )

    cache_mgr = CacheManager(cache_ttl_minutes=60)

    # Single zone selector for historical analysis
    st.subheader("Select Zone for Historical Analysis")
    analysis_zone = st.text_input(
        "Enter zone code (e.g., DE, FR, IT)",
        value="DE",
    )

    if not analysis_zone:
        st.warning("Enter a zone code to proceed.")
        return

    tab1, tab2, tab3, tab4 = st.tabs(
        ["Carbon Intensity Trend", "Power Mix Trend", "Renewable Composition", "Import/Export Balance"]
    )

    with tab1:
        st.subheader(f"Carbon Intensity History – {analysis_zone}")

        with st.spinner("Loading carbon history..."):
            carbon_hist = cache_mgr.get_or_fetch(
                f"em_carbon_history_{analysis_zone}",
                lambda z=analysis_zone: fetch_electricity_maps_carbon_history(zone=z, hours=24),
                force_refresh=refresh_cache,
            )

        if carbon_hist is not None and not carbon_hist.empty:
            if "timestamp" in carbon_hist.columns and "carbonIntensity" in carbon_hist.columns:
                carbon_hist_sorted = carbon_hist.sort_values("timestamp")
                fig = px.line(
                    carbon_hist_sorted,
                    x="timestamp",
                    y="carbonIntensity",
                    title=f"24h Carbon Intensity Trend – {analysis_zone}",
                    labels={"carbonIntensity": "gCO₂/kWh", "timestamp": "Time"},
                )
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(carbon_hist_sorted[["timestamp", "carbonIntensity"]], width="stretch")
            else:
                st.warning("Carbon history data missing expected columns.")
        else:
            st.info(f"No carbon history available for {analysis_zone}.")

    with tab2:
        st.subheader(f"Power Mix Trend – {analysis_zone}")

        with st.spinner("Loading power history..."):
            power_hist = cache_mgr.get_or_fetch(
                f"em_power_history_{analysis_zone}",
                lambda z=analysis_zone: fetch_electricity_maps_power_past(zone=z),
                force_refresh=refresh_cache,
            )

        if power_hist is not None and not power_hist.empty:
            if "timestamp" in power_hist.columns:
                power_hist_sorted = power_hist.sort_values("timestamp")
                
                # Try to plot fuel mix over time
                fuel_cols = [
                    col for col in power_hist_sorted.columns
                    if col in ["coal", "gas", "hydro", "nuclear", "wind", "solar", "biomass"]
                ]
                
                if fuel_cols:
                    fig = px.area(
                        power_hist_sorted,
                        x="timestamp",
                        y=fuel_cols,
                        title=f"Power Mix Trend – {analysis_zone}",
                        labels={"timestamp": "Time", "value": "MW"},
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(power_hist_sorted, width="stretch")
            else:
                st.warning("Power history data missing timestamp column.")
        else:
            st.info(f"No power history available for {analysis_zone}.")

    with tab3:
        st.subheader(f"Renewable Composition – {analysis_zone}")
        st.info("Shows renewable percentage evolution over the past 24h (if available).")

        if power_hist is not None and not power_hist.empty:
            if "renewablePercentage" in power_hist.columns:
                power_hist_sorted = power_hist.sort_values("timestamp")
                fig = px.line(
                    power_hist_sorted,
                    x="timestamp",
                    y="renewablePercentage",
                    title=f"Renewable % Trend – {analysis_zone}",
                    labels={"renewablePercentage": "Renewable %", "timestamp": "Time"},
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("Renewable percentage not available in power history.")
        else:
            st.info("No power history to compute renewable composition.")

    with tab4:
        st.subheader(f"Import/Export Balance – {analysis_zone}")

        if power_hist is not None and not power_hist.empty:
            if "powerImportTotal" in power_hist.columns and "powerExportTotal" in power_hist.columns:
                power_hist_sorted = power_hist.sort_values("timestamp")
                
                # Create balance chart
                power_hist_sorted["balance_mw"] = (
                    power_hist_sorted["powerImportTotal"] - power_hist_sorted["powerExportTotal"]
                )
                
                fig = px.bar(
                    power_hist_sorted,
                    x="timestamp",
                    y=["powerImportTotal", "powerExportTotal"],
                    title=f"Import/Export Balance – {analysis_zone}",
                    labels={"timestamp": "Time", "value": "MW"},
                    barmode="group",
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.dataframe(
                    power_hist_sorted[["timestamp", "powerImportTotal", "powerExportTotal"]],
                    width="stretch"
                )
            else:
                st.warning("Import/export data not available.")
        else:
            st.info("No power history to compute import/export balance.")

# ============================================================================
# SOCIO-ECONOMIC INDICATORS PAGE
# ============================================================================

def socioeconomic_page(selected_countries, refresh_cache):
    """Socio-Economic Indicators Comparison Page"""
    st.title("🌍 Socio-Economic Indicators")
    
    st.info(
        "Compare key socio-economic indicators across countries. "
        "Data sourced from World Bank API (no key needed)."
    )
    
    from modules.socioeconomic_fetcher import (
        fetch_multiple_socioeconomic_indicators,
        fetch_socioeconomic_indicator,
        list_available_indicators,
        pivot_indicators_by_country,
        get_indicator_description,
    )
    
    cache_mgr = CacheManager(cache_ttl_minutes=120)
    
    # Country selector
    st.subheader("Select Countries to Compare")
    
    default_countries = ["USA", "GBR", "DEU", "FRA", "CHN", "IND", "BRA"]
    selected_countries = st.multiselect(
        "Countries (ISO3 codes, e.g., USA, GBR, DEU)",
        options=default_countries + ["JPN", "KOR", "AUS", "CAN", "MEX"],
        default=default_countries[:4],
    )
    
    if not selected_countries:
        st.warning("Select at least one country.")
        return
    
    st.divider()
    
    # Tab 1: Latest Snapshot (comparison table)
    tab1, tab2, tab3, tab4 = st.tabs(
        ["Latest Snapshot", "Indicator Trends", "Heatmap Analysis", "Custom Comparison"]
    )
    
    with tab1:
        st.subheader("Latest Year Snapshot")
        st.caption("Most recent data available for each indicator by country")
        
        # Default indicators for snapshot
        snapshot_indicators = [
            "NY.GDP.PCAP.CD",
            "SP.DYN.LE00.IN",
            "SL.UEM.TOTL.ZS",
            "SE.ADT.LITR.ZS",
            "EN.ATM.CO2E.PC",
            "SI.POV.GINI",
        ]
        
        with st.spinner("Loading snapshot data..."):
            snapshot_data = cache_mgr.get_or_fetch(
                f"socio_snapshot_{'_'.join(selected_countries)}",
                lambda: fetch_multiple_socioeconomic_indicators(
                    snapshot_indicators,
                    countries=selected_countries,
                    most_recent=1
                ),
                force_refresh=refresh_cache,
            )
        
        if snapshot_data:
            # Pivot to comparison format
            comparison_df = pivot_indicators_by_country(snapshot_data)
            
            if not comparison_df.empty:
                st.dataframe(comparison_df.fillna("N/A"), width="stretch")
                
                # Download button
                csv = comparison_df.to_csv()
                st.download_button(
                    "📥 Download Comparison (CSV)",
                    csv,
                    "socio_comparison.csv",
                )
            else:
                st.info("No data available for selected countries.")
        else:
            st.warning("Could not fetch snapshot data.")
    
    with tab2:
        st.subheader("Indicator Trends Over Time")
        st.caption("View historical trend for a single indicator across countries")
        
        available_indicators = list_available_indicators()
        selected_indicator = st.selectbox(
            "Select Indicator",
            options=list(available_indicators.keys()),
            format_func=lambda x: available_indicators[x],
        )
        
        years_back = st.slider("Years of history", min_value=1, max_value=20, value=10)
        
        with st.spinner(f"Loading {selected_indicator} trend..."):
            trend_data = cache_mgr.get_or_fetch(
                f"socio_trend_{selected_indicator}_{'_'.join(selected_countries)}_{years_back}y",
                lambda: fetch_socioeconomic_indicator(
                    selected_indicator,
                    countries=selected_countries,
                    most_recent=years_back
                ),
                force_refresh=refresh_cache,
            )
        
        if trend_data is not None and not trend_data.empty:
            # Create line chart
            trend_data_sorted = trend_data.sort_values("year")
            
            fig = px.line(
                trend_data_sorted,
                x="year",
                y="value",
                color="country",
                title=f"{available_indicators[selected_indicator]} Trend",
                labels={"year": "Year", "value": selected_indicator, "country": "Country"},
                markers=True,
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            st.write("Historical Data")
            st.dataframe(
                trend_data_sorted[["country", "year", "value"]].sort_values(["country", "year"]),
                width="stretch"
            )
        else:
            st.info(f"No historical data available for {available_indicators[selected_indicator]}.")
    
    with tab3:
        st.subheader("Heatmap Analysis")
        st.caption("Visual comparison of multiple indicators across countries")
        
        # Select which indicators to include in heatmap
        heatmap_indicators = st.multiselect(
            "Select indicators for heatmap",
            options=list(list_available_indicators().keys()),
            default=[
                "NY.GDP.PCAP.CD",
                "SP.DYN.LE00.IN",
                "SE.ADT.LITR.ZS",
                "EN.ATM.CO2E.PC",
            ],
        )
        
        if heatmap_indicators:
            with st.spinner("Fetching heatmap data..."):
                heatmap_data = cache_mgr.get_or_fetch(
                    f"socio_heatmap_{'_'.join(heatmap_indicators)}_{'_'.join(selected_countries)}",
                    lambda: fetch_multiple_socioeconomic_indicators(
                        heatmap_indicators,
                        countries=selected_countries,
                        most_recent=1
                    ),
                    force_refresh=refresh_cache,
                )
            
            if heatmap_data:
                # Prepare data for heatmap
                heatmap_df = pivot_indicators_by_country(heatmap_data)
                
                if not heatmap_df.empty:
                    # Normalize data (0-1 scale) for better heatmap visualization
                    heatmap_normalized = heatmap_df.copy()
                    for col in heatmap_normalized.columns:
                        # Skip non-numeric columns (e.g. country names)
                        if not pd.api.types.is_numeric_dtype(heatmap_normalized[col]):
                            continue

                        min_val = pd.to_numeric(heatmap_normalized[col], errors="coerce").min()
                        max_val = pd.to_numeric(heatmap_normalized[col], errors="coerce").max()

                        if pd.isna(min_val) or pd.isna(max_val) or max_val <= min_val:
                            continue

                        col_numeric = pd.to_numeric(heatmap_normalized[col], errors="coerce")
                        heatmap_normalized[col] = (col_numeric - min_val) / (max_val - min_val)

                    
                    numeric_heatmap = heatmap_normalized.select_dtypes(include=["number"])
                    if numeric_heatmap.empty:
                        st.info("No numeric data available to build heatmap.")
                    else:
                        fig = px.imshow(
                            numeric_heatmap,
                            labels=dict(x="Indicator", y="Country", color="Normalized Value"),
                            title="Socio-Economic Indicators Heatmap (Normalized)",
                            color_continuous_scale="RdYlGn",
                            aspect="auto",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    
                    st.caption(
                        "Green = higher values, Red = lower values (normalized 0-1). "
                        "Use to quickly identify relative strengths and weaknesses."
                    )
                else:
                    st.info("No data for selected indicators.")
        else:
            st.info("Select at least one indicator.")
    
    with tab4:
        st.subheader("Custom Comparison")
        st.caption("Create your own custom comparison (up to 2 indicators)")
        
        col1, col2 = st.columns(2)
        
        with col1:
            available_ind = list_available_indicators()
            indicator_x = st.selectbox(
                "Indicator X-axis",
                options=list(available_ind.keys()),
                format_func=lambda x: available_ind[x],
                key="custom_x",
            )
        
        with col2:
            indicator_y = st.selectbox(
                "Indicator Y-axis",
                options=list(available_ind.keys()),
                format_func=lambda x: available_ind[x],
                key="custom_y",
            )
        
        if st.button("Generate Scatter Plot"):
            with st.spinner("Fetching data..."):
                custom_data = cache_mgr.get_or_fetch(
                    f"socio_custom_{indicator_x}_{indicator_y}_{'_'.join(selected_countries)}",
                    lambda: fetch_multiple_socioeconomic_indicators(
                        [indicator_x, indicator_y],
                        countries=selected_countries,
                        most_recent=1
                    ),
                    force_refresh=refresh_cache,
                )
            
            if custom_data and custom_data[indicator_x] is not None and custom_data[indicator_y] is not None:
                # Merge the two dataframes
                df_x = custom_data[indicator_x][["country", "value"]].rename(
                    columns={"value": "x_value"}
                )
                df_y = custom_data[indicator_y][["country", "value"]].rename(
                    columns={"value": "y_value"}
                )
                
                scatter_df = df_x.merge(df_y, on="country", how="inner")
                
                if not scatter_df.empty:
                    fig = px.scatter(
                        scatter_df,
                        x="x_value",
                        y="y_value",
                        text="country",
                        title=f"{available_ind[indicator_x]} vs {available_ind[indicator_y]}",
                        labels={
                            "x_value": available_ind[indicator_x],
                            "y_value": available_ind[indicator_y],
                        },
                    )
                    fig.update_traces(textposition="top center")
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("No matching data for both indicators.")
            else:
                st.warning("Could not fetch data for selected indicators.")


def newsletter_page(refresh_cache):
    """Newsletter Generation Page"""
    st.title("📰 Newsletter Engine")

    st.info(
        """
        Automated newsletter generation from last 72 hours of *real* flow data:

        - Surge detection using 7-day rolling averages
        - Markdown preview and PDF export
        """
    )

    cache_mgr = CacheManager(cache_ttl_minutes=120)

    col1, col2 = st.columns([3, 1])
    with col1:
        st.subheader("Newsletter Preview")
    with col2:
        if st.button("🔄 Generate New"):
            cache_mgr.clear_cache("newsletter_content")
            st.rerun()

    with st.spinner("Generating newsletter..."):
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(hours=72)

            entso_data = cache_mgr.get_or_fetch(
                "newsletter_flows",
                lambda: fetch_entso_data((start_date.date(), end_date.date())),
                force_refresh=refresh_cache,
            )

            if entso_data is None or entso_data.empty:
                st.info("No ENTSO-E data available for the last 72 hours.")
                return

            alerts = detect_surge_alerts(entso_data, deviation_threshold=20)
            newsletter_md = generate_newsletter(entso_data, alerts)
            st.success("Newsletter generated")
        except Exception as e:
            st.error(f"Error generating newsletter: {str(e)}")
            return

    st.markdown(newsletter_md)
    st.divider()

    col1, col2 = st.columns(2)
    with col1:
        st.download_button(
            "📄 Download Markdown",
            newsletter_md,
            "newsletter.md",
            "text/markdown",
        )
    with col2:
        if st.button("📊 Generate PDF"):
            try:
                pdf_path = export_newsletter_pdf(newsletter_md, entso_data)
                with open(pdf_path, "rb") as pdf_file:
                    st.download_button(
                        "📋 Download PDF",
                        pdf_file,
                        "newsletter.pdf",
                        "application/pdf",
                    )
            except Exception as e:
                st.error(f"Error generating PDF: {str(e)}")


def data_management_page():
    """Data Management Page"""
    st.title("⚙️ Data Management")

    tab1, tab2, tab3 = st.tabs(["Upload Data", "Templates", "Data Status"])

    with tab1:
        st.subheader("Upload Custom Data")

        data_type = st.selectbox(
            "Select Data Type",
            ["Cross-border Flows", "Generation Mix", "Power Stations", "Grid Infrastructure"],
        )

        uploaded_file = st.file_uploader(
            f"Upload {data_type} data", type=["csv", "xlsx", "json"]
        )

        if uploaded_file:
            try:
                if uploaded_file.type == "text/csv":
                    df = pd.read_csv(uploaded_file)
                elif uploaded_file.type == "application/json":
                    df = pd.read_json(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)

                st.write(f"Preview ({len(df)} rows)")
                st.dataframe(df.head(20), use_container_width=True)

                if st.button("✅ Confirm Upload"):
                    # Here you would persist to DB or filesystem
                    st.success(f"Uploaded {len(df)} records successfully")
            except Exception as e:
                st.error(f"Error processing file: {str(e)}")

    with tab2:
        st.subheader("Data Templates (Schema Only)")
        st.write("Download empty templates (column headers only).")

        flows_template = pd.DataFrame(
            {
                "timestamp": pd.Series(dtype="str"),
                "from_country": pd.Series(dtype="str"),
                "to_country": pd.Series(dtype="str"),
                "flow_mw": pd.Series(dtype="float"),
                "capacity_mw": pd.Series(dtype="float"),
                "source": pd.Series(dtype="str"),
            }
        )

        generation_template = pd.DataFrame(
            {
                "date": pd.Series(dtype="str"),
                "country": pd.Series(dtype="str"),
                "coal_mwh": pd.Series(dtype="float"),
                "gas_mwh": pd.Series(dtype="float"),
                "nuclear_mwh": pd.Series(dtype="float"),
                "hydro_mwh": pd.Series(dtype="float"),
                "wind_mwh": pd.Series(dtype="float"),
                "solar_mwh": pd.Series(dtype="float"),
                "biomass_mwh": pd.Series(dtype="float"),
                "total_mwh": pd.Series(dtype="float"),
            }
        )

        col1, col2 = st.columns(2)
        with col1:
            csv = flows_template.to_csv(index=False)
            st.download_button(
                "📥 Cross-border Flows Template (Empty)",
                csv,
                "flows_template.csv",
            )
        with col2:
            csv = generation_template.to_csv(index=False)
            st.download_button(
                "📥 Generation Mix Template (Empty)",
                csv,
                "generation_template.csv",
            )

    with tab3:
        st.subheader("Data Source Status (Real Checks)")

        import time
        from modules.data_fetcher import (
            fetch_eia_data,
            fetch_entso_data,
            fetch_ember_data,
            fetch_electricity_maps_data,
            fetch_owid_data_local,
            fetch_world_bank_data,
        )

        def check_source(name, func):
            try:
                df = func()
                ok = df is not None and not df.empty
                status = "✅ OK" if ok else "⚠️ No data"
                records = len(df) if ok else 0
            except Exception:
                status = "❌ Error"
                records = 0
            elapsed = time.strftime("%Y-%m-%d %H:%M:%S")
            return {
                "Source": name,
                "Status": status,
                "Last Checked": elapsed,
                "Records": records,
            }

        checks = []
        # Keep these light and infrequent; consider caching if needed.
        checks.append(
            check_source(
                "EIA", lambda: fetch_eia_data((datetime.now(), datetime.now()))
            )
        )
        checks.append(
            check_source(
                "ENTSO-E", lambda: fetch_entso_data((datetime.now(), datetime.now()))
            )
        )
        checks.append(
            check_source(
                "Ember",
                lambda: fetch_ember_data(
                    (datetime.now().year - 1, datetime.now().year),  # last 2 years
                    entity_code="BRA",                             # or a default like 'BRA'
                ),
            )
        )
        checks.append(
            check_source("Electricity Maps", lambda: fetch_electricity_maps_data())
        )
        checks.append(check_source("Our World in Data", lambda: fetch_owid_data_local()))
        checks.append(
            check_source("World Bank", lambda: fetch_world_bank_data())
        )

        status_df = pd.DataFrame(checks)
        st.dataframe(status_df, use_container_width=True)


def api_config_page():
    """API Configuration Page"""
    st.title("📋 API Configuration")

    st.info("Configure your API tokens and basic database settings.")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("API Tokens")

        with st.expander("EIA API"):
            st.write("U.S. Energy Information Administration")
            st.text_input("EIA Token", type="password", key="eia_token")

        with st.expander("ENTSO-E API"):
            st.write("European Network of Transmission System Operators")
            st.text_input("ENTSO-E Token", type="password", key="entso_token")

        with st.expander("Electricity Maps API"):
            st.write("Real-time Electricity Data")
            st.text_input("Electricity Maps Token", type="password", key="emaps_token")

        with st.expander("Ember API"):
            st.write("Global Electricity Dashboard")
            st.text_input("Ember Token", type="password", key="ember_token")

        with st.expander("NewsAPI"):
            st.write("News Aggregation for Cross-border Electricity")
            st.text_input("NewsAPI Token", type="password", key="news_token")

    with col2:
        st.subheader("Quick Status")
        st.caption(
            "Actual API health is shown on ⚙️ Data Management → Data Status. "
            "This panel is informational only."
        )

    st.divider()

    st.subheader("Database Configuration")
    db_type = st.selectbox("Database Type", ["SQLite", "PostgreSQL", "MySQL"])
    db_path = st.text_input("Database Path/Connection String", "~/electricity_data.db")

    if st.button("Test Database Connection"):
        # Placeholder – you should implement a real connection test here.
        st.info(f"Database test not implemented yet for {db_type}.")


# ============================================================================
# MAIN CONTENT ROUTING
# ============================================================================

if page == "🏠 Home":
    home_page(date_range, selected_countries)
elif page == "📊 Historical Data Analysis":
    historical_data_page(date_range, selected_countries, refresh_cache)
elif page == "🔴 Live Data Feed":
    live_data_page(selected_countries, refresh_cache)
elif page == "🗺️ Interconnection Map":
    map_page(date_range, selected_countries, refresh_cache)
elif page == "📈 Analytics & Insights":
    analytics_page(date_range, selected_countries, refresh_cache)
elif page == "🌍 Socio-Economic Indicators":
    socioeconomic_page(selected_countries, refresh_cache)
elif page == "📰 Newsletter Engine":
    newsletter_page(refresh_cache)
elif page == "⚙️ Data Management":
    data_management_page()
elif page == "📋 API Configuration":
    api_config_page()

# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    st.write("Application initialized.")
