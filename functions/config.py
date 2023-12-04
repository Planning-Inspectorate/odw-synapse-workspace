"""
Config file to hold variables used by function apps and other functions
"""
import datetime
from azure.identity import DefaultAzureCredential
from pydantic import ConfigDict

STORAGE = "https://pinsstodwdevuks9h80mb.blob.core.windows.net"
CONTAINER = "odw-raw/odt/test"
ODT_NAMESPACE = "pins-sb-back-office-dev-ukw-001.servicebus.windows.net"
ODW_NAMESPACE = "pins-sb-odw-dev-uks-b9rt9m.servicebus.windows.net"
SERVICE_USER_SUBSCRIPTION = "service-user"
SERVICE_USER_TOPIC = "service-user"
NSIP_SUBSCRIPTION = "nsip-project"
NSIP_TOPIC = "nsip-project"
MAX_MESSAGE_COUNT = 10
UTC_TIMESTAMP = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(tzinfo=datetime.timezone.utc)
    .isoformat()
)
CREDENTIAL = DefaultAzureCredential()
MODEL_CONFIG = ConfigDict(validate_assignment=True, extra="forbid")
