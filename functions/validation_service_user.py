"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages
"""

import model_service_user

from pydantic import BaseModel, ValidationError
from servicebus_funcs import get_messages
import config

_NAMESPACE = config.ODW_NAMESPACE_DEV
_SUBSCRIPTION = config.SERVICE_USER_SUBSCRIPTION
_TOPIC = config.SERVICE_USER_TOPIC
_CREDENTIAL = config.CREDENTIAL
_MESSAGES = model_service_user.ServiceUser
_MAX_MESSAGE_COUNT = config.MAX_MESSAGE_COUNT


def validate() -> list:
    """
    Function to validate a list of servicebus messages
    """

    _data = get_messages(
    _NAMESPACE, _CREDENTIAL, _TOPIC, _SUBSCRIPTION, _MAX_MESSAGE_COUNT
    )

    class MessageInstances(BaseModel):

        """
        Represents a pydantic model for a list of ServiceUser instances.

        Args:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.

        Attributes:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.
        """

        messagedata: list[_MESSAGES]

    try:
        MessageInstances(messagedata=_data)
        print("VALIDATION SUCCEEDED!")
        print(f"{len(_data)} MESSAGES PROCESSED")
        return _data
    except ValidationError as e:
        print(e)
        raise e
