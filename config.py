"""
Configuration Module - Centralized configuration management
"""

import os
from datetime import timedelta
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

class DatabaseConfig:
    """Database settings"""
    
    # SQLite (default - simple, no setup needed)
    TYPE = os.getenv("DATABASE_TYPE", "sqlite")
    URL = os.getenv("DATABASE_URL", "sqlite:///electricity_data.db")
    
    # For PostgreSQL: postgresql://user:password@localhost/dbname
    # For MySQL: mysql+pymysql://user:password@localhost/dbname
    
    # Connection pool settings
    POOL_SIZE = 10
    MAX_OVERFLOW = 20
    ECHO = False  # Set to True for SQL debugging

# ============================================================================
# CACHE CONFIGURATION
# ============================================================================

class CacheConfig:
    """Cache settings"""
    
    # Cache type: 'file', 'memory', 'redis'
    TYPE = os.getenv("CACHE_TYPE", "file")
    
    # TTL (Time To Live) in minutes for different data types
    TTL_DEFAULT = int(os.getenv("CACHE_TTL_MINUTES", "30"))
    TTL_LIVE = 5  # Live data: 5 minutes
    TTL_HISTORICAL = 120  # Historical: 2 hours
    TTL_NEWS = 60  # News: 1 hour
    
    # File cache location
    LOCATION = os.getenv("CACHE_LOCATION", ".cache")
    
    # Redis settings (if using Redis)
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# ============================================================================
# STREAMLIT CONFIGURATION
# ============================================================================

class StreamlitConfig:
    """Streamlit UI settings"""
    
    PAGE_TITLE = "Cross-border Electricity Interconnection MIS"
    PAGE_ICON = "⚡"
    LAYOUT = "wide"
    INITIAL_SIDEBAR_STATE = "expanded"
    THEME = os.getenv("STREAMLIT_THEME", "light")  # 'light' or 'dark'
    
    # Color scheme
    PRIMARY_COLOR = "#208A80"  # Teal
    BACKGROUND_COLOR = "#FAFAF8"  # Cream
    SECONDARY_BACKGROUND_COLOR = "#F0F2F6"  # Light gray
    TEXT_COLOR = "#134252"  # Dark slate
    FONT_FAMILY = "sans serif"

# ============================================================================
# API CONFIGURATION
# ============================================================================

class APIConfig:
    """API endpoint and timeout settings"""
    
    # Request timeout (seconds)
    REQUEST_TIMEOUT = 30
    REQUEST_TIMEOUT_LONG = 60
    
    # Retry settings
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds
    
    # Rate limiting
    RATE_LIMIT_REQUESTS = 100
    RATE_LIMIT_PERIOD = 3600  # 1 hour

# ============================================================================
# DATA SOURCE CONFIGURATION
# ============================================================================

class DataSourceConfig:
    """Configuration for each data source"""
    
    # Date range defaults
    DEFAULT_HISTORICAL_DAYS = 365
    DEFAULT_LOOKBACK_DAYS = 7
    
    # Data validation thresholds
    MIN_DATAPOINTS = 10  # Minimum to consider data valid
    ANOMALY_THRESHOLD = 20  # % deviation to flag as anomaly
    
    # Country codes mapping (ISO 3166-1 alpha-2)
    EU_COUNTRIES = [
        'DE', 'FR', 'ES', 'IT', 'PL', 'NL', 'BE', 'AT', 'CZ', 'PT',
        'DK', 'SE', 'NO', 'FI', 'GR', 'RO', 'HU', 'SK', 'HR', 'SI',
        'BG', 'LV', 'LT', 'EE', 'IE', 'UK', 'CH', 'LU', 'MT', 'CY'
    ]
    
    ENTSO_E_COUNTRIES = {
        'Germany': '10Y1001A1001A63L',
        'France': '10YFR-RTE------C',
        'Spain': '10YES-REE------0',
        'Italy': '10Y1001A1001A73V',
        'Austria': '10Y1001A1001A64V',
        'Poland': '10YPL-ECJ------0',
        'Netherlands': '10YNL----------L',
        'Belgium': '10YBE----------2',
        'Czech Republic': '10YCZ-CEPS-----N',
        'Denmark': '10YDK-1--------W',
        'Sweden': '10YSE-1--------K',
        'Norway': '10YNO-1--------2',
        'Portugal': '10YPT-REN------W',
        'Greece': '10YGR-HTSO------Y',
    }

# ============================================================================
# FUEL TYPE CONFIGURATION
# ============================================================================

class FuelConfig:
    """Fuel types and color mappings"""
    
    FUEL_TYPES = [
        'coal', 'gas', 'nuclear', 'hydro', 'wind', 'solar', 'biomass', 'other'
    ]
    
    # Color mapping for visualizations
    FUEL_COLORS = {
        'coal': '#2C2C2C',        # Black
        'gas': '#FF9800',         # Orange
        'nuclear': '#FDD835',     # Yellow
        'hydro': '#1E88E5',       # Blue
        'wind': '#66BB6A',        # Green
        'solar': '#FFA726',       # Orange
        'biomass': '#8D6E63',     # Brown
        'other': '#757575'        # Gray
    }

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

class LogConfig:
    """Logging settings"""
    
    LEVEL = os.getenv("LOG_LEVEL", "INFO")
    FILE = os.getenv("LOG_FILE", "logs/app.log")
    MAX_SIZE = 10_000_000  # 10 MB
    BACKUP_COUNT = 5
    FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# ============================================================================
# VALIDATION CONFIGURATION
# ============================================================================

class ValidationConfig:
    """Data validation rules"""
    
    # Flow validation
    MIN_FLOW = -99999  # MW
    MAX_FLOW = 99999   # MW
    
    # Capacity validation
    MIN_CAPACITY = 0
    MAX_CAPACITY = 99999
    
    # Percentage validation (0-100)
    MIN_PERCENTAGE = 0
    MAX_PERCENTAGE = 100
    
    # Timestamp validation
    FUTURE_DAYS_ALLOWED = 7  # Allow forecasts up to 7 days
    PAST_DAYS_ALLOWED = 365 * 10  # Allow up to 10 years of history

# ============================================================================
# NEWSLETTER CONFIGURATION
# ============================================================================

class NewsletterConfig:
    """Newsletter generation settings"""
    
    # Surge alert threshold
    SURGE_THRESHOLD = 20  # % deviation to trigger alert
    
    # Newsletter generation
    FREQUENCY = "daily"  # 'daily', 'weekly'
    GENERATION_HOUR = 18  # 6 PM
    
    # Email settings (if implementing email)
    SMTP_SERVER = os.getenv("SMTP_SERVER", "")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    SENDER_EMAIL = os.getenv("SENDER_EMAIL", "")
    SENDER_PASSWORD = os.getenv("SENDER_PASSWORD", "")

# ============================================================================
# EXPORT CONFIGURATION
# ============================================================================

class ExportConfig:
    """File export settings"""
    
    # Supported formats
    FORMATS = ['csv', 'excel', 'json', 'pdf']
    
    # Default export location
    EXPORT_DIR = "./exports"
    
    # PDF settings
    PDF_PAGE_SIZE = "A4"
    PDF_MARGIN = 10  # mm

# ============================================================================
# FEATURE FLAGS
# ============================================================================

class FeatureFlags:
    """Enable/disable features"""
    
    # Data sources to enable
    ENABLE_EIA = True
    ENABLE_ENTSO_E = True
    ENABLE_ELECTRICITY_MAPS = True
    ENABLE_EMBER = True
    ENABLE_OWID = True
    ENABLE_WORLD_BANK = True
    ENABLE_NEWS = True
    
    # Features
    ENABLE_LIVE_DATA = True
    ENABLE_HISTORICAL_ANALYSIS = True
    ENABLE_ANOMALY_DETECTION = True
    ENABLE_NEWSLETTER = True
    ENABLE_FORECASTING = False  # Not implemented yet
    ENABLE_USER_UPLOADS = True
    ENABLE_API_CONFIG = True

# ============================================================================
# SUMMARY CONFIGURATION CLASS
# ============================================================================

class Config:
    """Main configuration class - combines all settings"""
    
    DATABASE = DatabaseConfig
    CACHE = CacheConfig
    STREAMLIT = StreamlitConfig
    API = APIConfig
    DATA_SOURCE = DataSourceConfig
    FUEL = FuelConfig
    LOG = LogConfig
    VALIDATION = ValidationConfig
    NEWSLETTER = NewsletterConfig
    EXPORT = ExportConfig
    FEATURES = FeatureFlags
    
    # Global settings
    DEBUG = os.getenv("DEBUG", "False").lower() == "true"
    ENVIRONMENT = os.getenv("ENVIRONMENT", "development")  # 'development', 'staging', 'production'
