# docker/superset/superset_config.py
import os

# Clé secrète
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "supersecretkey_lakehouse_2024")

# Base de données Superset
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg2://superset:superset123@superset-db:5432/superset"
)

# Connexion Trino (ajoutée automatiquement au démarrage)
TRINO_CONNECTION = {
    "database_name": "Trino Lakehouse",
    "sqlalchemy_uri": "trino://trino:8080/iceberg",
}

# Langues
BABEL_DEFAULT_LOCALE = "fr"
LANGUAGES = {
    "fr": {"flag": "fr", "name": "Français"},
    "en": {"flag": "us", "name": "English"},
}

# Options
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_TIMEOUT = 300
ENABLE_PROXY_FIX = True

# Cache (simple en mémoire pour dev)
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}
