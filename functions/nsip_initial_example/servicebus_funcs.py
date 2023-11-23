import logging
import datetime
from azure.servicebus import ServiceBusClient
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
import json
import pprint

NAMESPACE = 'https://pins-sb-odw-dev-uks-b9rt9m.servicebus.windows.net'
SUBSCRIPTION = 'nsip-exam-timetable'
TOPIC = 'nsip-exam-timetable'
MAX_MESSAGE_COUNT = 10
STORAGE = 'https://pinsstodwdevuks9h80mb.blob.core.windows.net'
CONTAINER = 'odw-raw/odt/test'
CREDENTIAL = DefaultAzureCredential()
UTC_TIMESTAMP = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(tzinfo=datetime.timezone.utc)
    .isoformat()
)
FILENAME = f'messages_{UTC_TIMESTAMP}.json'

def get_messages(namespace: str,
                 credential,
                 topic: str,
                 subscription: str,
                 max_message_count: int) -> list:

    print("Creating Servicebus client...")

    servicebus_client = ServiceBusClient(
    fully_qualified_namespace=namespace,
    credential=credential
)

    print("Servicebus client created")

    print("Creating receiver object...")

    with servicebus_client:
        subscription_receiver = servicebus_client.get_subscription_receiver(
        topic_name=topic,
        subscription_name=subscription
        )

        print("Peeking messages...")

        with subscription_receiver:
            messages_list = []
            messages_peek = subscription_receiver.peek_messages(max_message_count=max_message_count)
            for message in messages_peek:
                message_body = json.loads(str(message))
                messages_list.append(message_body)

    print(f'{len(messages_list)} messages received from topic')

    return messages_list

def send_to_storage(account_url: str,
                    credential,
                    container: str,
                    filename: str,
                    data) -> None:

    print("Creating blob service client...")

    blob_service_client = BlobServiceClient(
        account_url,
        credential
    )

    print("Blob service client created")

    blob_client = blob_service_client.get_blob_client(container, blob=filename)

    print("Converting data to json format...")

    json_data = json.dumps(data)

    print("Data converted to json")

    print("Uploading file to storage...")

    blob_client.upload_blob(json_data, overwrite=True)

    print(f"JSON file '{filename}' uploaded to Azure Blob Storage.")