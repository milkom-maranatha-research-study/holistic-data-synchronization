import os
import logging


DEV_MODE = os.environ.get('DEV_MODE', True)
DEBUG_MODE = os.environ.get('DEBUG_MODE', True)

# Metabase API
METABASE_URL = os.environ.get('METABASE_URL', '')
METABASE_SERVICE_ACCOUNT = os.environ.get('METABASE_SERVICE_ACCOUNT', '')
METABASE_SERVICE_ACCOUNT_PASSWORD = os.environ.get('METABASE_SERVICE_ACCOUNT_PASSWORD', '')

# Holistic Backend API
HOLISTIC_BACKEND_URL = os.environ.get('HOLISTIC_BACKEND_URL', '')
HOLISTIC_SERVICE_ACCOUNT = os.environ.get('HOLISTIC_SERVICE_ACCOUNT', '')
HOLISTIC_SERVICE_ACCOUNT_PASSWORD = os.environ.get('HOLISTIC_SERVICE_ACCOUNT_PASSWORD', '')


def configure_logging():
    if DEBUG_MODE:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
