import os
from pathlib import Path

DEFAULT_SERVICE_NAME = 'Dentascan Assistant Service'
DEFAULT_SERVICE_AUTO_START = True
DEFAULT_SERVICE_API_HOST = '127.0.0.1'
DEFAULT_SERVICE_API_PORT = 8085

DEFAULT_SERVICE_ROOT_PATH = 'C:\\Users\\spoof\\Desktop'
DEFAULT_SERVICE_STAGING_PATH = 'C:\\Users\\spoof\\Desktop'
DEFAULT_INSTITUTION_NAME = 'Dentascan'

DEFAULT_PACS_MAX_UPLOAD_BPS = None  # None = unlimited

SERVICE_NAME = 'Dentascan Assistant Service'
SERVICE_AUTO_START = True
SERVICE_API_HOST = '127.0.0.1'
SERVICE_API_PORT = 8085

SERVICE_ROOT_PATH = 'C:\\Users\\spoof\\Desktop'
SERVICE_STAGING_PATH = 'C:\\Users\\spoof\\Desktop'
INSTITUTION_NAME = 'Dentascan'

PACS_MAX_UPLOAD_BPS = None

# PACS (Keycloak client_credentials)
PACS_BASE_URL = "https://pacs.dentascan.net"
PACS_TOKEN_URL = "https://auth.dentascan.net/realms/dentascan/protocol/openid-connect/token"
PACS_CLIENT_ID = "pacs-uploader"
PACS_CLIENT_SECRET = "yLEZlI0UE44ezsaNNPBX74baYdQi5ZoU"

