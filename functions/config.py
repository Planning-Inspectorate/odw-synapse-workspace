"""
Config file to hold variables used by function apps and other functions
"""

import datetime
from azure.identity import DefaultAzureCredential
from pydantic import ConfigDict

STORAGE_DEV = "https://pinsstodwdevuks9h80mb.blob.core.windows.net"
STORAGE_PREPROD = "https://pinsstodwtestukswic3ai.blob.core.windows.net"
STORAGE_PROD = "https://pinsstodwprodukson83nw.blob.core.windows.net"
CONTAINER = "odw-raw/ServiceBus"
ODT_NAMESPACE_DEV = "pins-sb-back-office-dev-ukw-001.servicebus.windows.net"
ODT_NAMESPACE_PREPROD = "pins-sb-back-office-test-ukw-001.servicebus.windows.net"
ODT_NAMESPACE_PROD = "pins-sb-back-office-prod-ukw-001.servicebus.windows.net"
ODW_NAMESPACE_DEV = "pins-sb-odw-dev-uks-b9rt9m.servicebus.windows.net"
ODW_NAMESPACE_PREPROD = "pins-sb-odw-test-uks-hk2zun.servicebus.windows.net"
ODW_NAMESPACE_PROD = "pins-sb-odw-prod-uks-mwzecc.servicebus.windows.net"
SERVICE_USER_SUBSCRIPTION = "service-user"
SERVICE_USER_TOPIC = "service-user"
NSIP_PROJECT_SUBSCRIPTION = "nsip-project"
NSIP_PROJECT_TOPIC = "nsip-project"
MAX_MESSAGE_COUNT = 1000
UTC_TIMESTAMP = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(tzinfo=datetime.timezone.utc)
    .isoformat()
)
CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")
CREDENTIAL = DefaultAzureCredential()
MODEL_CONFIG = ConfigDict(validate_assignment=True, extra="forbid")
