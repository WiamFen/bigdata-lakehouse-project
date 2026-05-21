"""
Superset Configuration — Lakehouse Omnicanal
"""
import os

# Clé secrète
SECRET_KEY = "supersecretkey_lakehouse_2024"

# Base de données Superset
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset:superset123@superset-db:5432/superset"

# Cache
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# Fonctionnalités activées
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
}

# Row limit pour les requêtes
ROW_LIMIT = 100000
VIZ_ROW_LIMIT = 10000

# Timeout
SQLLAB_TIMEOUT = 300
SUPERSET_WEBSERVER_TIMEOUT = 300