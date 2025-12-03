# Superset configuration file
import os

# Security
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'dmviz-superset-secret-key-change-in-production')

# Database connection
SQLALCHEMY_DATABASE_URI = os.environ.get(
    'DATABASE_URL',
    'postgresql+psycopg2://superset:superset@postgres/superset'
)

# Flask-WTF flag for CSRF
WTF_CSRF_ENABLED = True

# Add endpoints that need to be exempt from CSRF protection
WTF_CSRF_EXEMPT_LIST = []

# Cache configuration
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
    'CACHE_KEY_PREFIX': 'superset_',
    'CACHE_REDIS_HOST': 'redis',
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1,
}

# Results backend for async queries
RESULTS_BACKEND = None

# Feature flags
FEATURE_FLAGS = {
    'ENABLE_TEMPLATE_PROCESSING': True,
    'DASHBOARD_NATIVE_FILTERS': True,
    'DASHBOARD_CROSS_FILTERS': True,
    'DASHBOARD_NATIVE_FILTERS_SET': True,
}

# Superset webserver configuration
SUPERSET_WEBSERVER_PORT = 8088

# SQL Lab settings
SQL_MAX_ROW = 100000
DISPLAY_MAX_ROW = 10000

# Enable uploading files
ENABLE_PROXY_FIX = True

