import azure.functions as func
import logging
import datetime
from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
import pprint
from servicebus_funcs import get_messages, send_to_storage

NAMESPACE = 'https://pins-sb-odw-dev-uks-b9rt9m.servicebus.windows.net'
SUBSCRIPTION = 'nsip-exam-timetable'
TOPIC = 'nsip-exam-timetable'
MAX_MESSAGE_COUNT = 10
STORAGE = 'https://pinsstodwdevuks9h80mb.blob.core.windows.net'
CONTAINER = 'odw-raw/odt/test'
CREDENTIAL = DefaultAzureCredential()

app = func.FunctionApp()

@app.function_name("nsip")
@app.route(route="nsip", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
def main(req: func.HttpRequest) -> func.HttpResponse:

    """
    Azure Function endpoint for handling HTTP requests.

    Args:
        req: An instance of `func.HttpRequest` representing the HTTP request.

    Returns:
        An instance of `func.HttpResponse` representing the HTTP response.
    """

    logging.info('FUNCTION STARTED...')

    UTC_TIMESTAMP = (
        datetime.datetime.now(datetime.timezone.utc)
        .replace(tzinfo=datetime.timezone.utc)
        .isoformat()
    )
    FILENAME = f'messages_{UTC_TIMESTAMP}.json'

    try:
        send_to_storage(
        account_url=STORAGE,
        credential=CREDENTIAL,
        container=CONTAINER,
        filename=FILENAME,
        data=get_messages(NAMESPACE,
                        CREDENTIAL,
                        TOPIC,
                        SUBSCRIPTION,
                        MAX_MESSAGE_COUNT)
        )    

    except Exception as e:
        logging.error(f'Error occurred: {str(e)}')

    return func.HttpResponse()