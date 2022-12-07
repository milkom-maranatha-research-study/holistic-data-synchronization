import os
import logging


DEV_MODE = os.environ.get('DEV_MODE', False)
DEBUG_MODE = os.environ.get('DEBUG_MODE', True)
SECRET_KEY = os.environ.get('SECRET_KEY', '')

# Metabase API
METABASE_URL = os.environ.get('METABASE_URL', '')
METABASE_SERVICE_ACCOUNT = os.environ.get('METABASE_SERVICE_ACCOUNT', '')
METABASE_SERVICE_ACCOUNT_PASSWORD = os.environ.get('METABASE_SERVICE_ACCOUNT_PASSWORD', '')

# Holistic Backend API
BACKEND_URL = os.environ.get('BACKEND_URL', '')
BACKEND_SERVICE_ACCOUNT = os.environ.get('BACKEND_SERVICE_ACCOUNT', '')
BACKEND_SERVICE_ACCOUNT_PASSWORD = os.environ.get('BACKEND_SERVICE_ACCOUNT_PASSWORD', '')


def configure_logging():
    if DEBUG_MODE:
        logging.root.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)
    else:
        logging.root.setLevel(logging.WARNING)
        logging.basicConfig(level=logging.WARNING)
