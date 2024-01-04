"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages
"""

from pydantic import BaseModel, ValidationError


def validate(data: list, model: BaseModel) -> list:
    """
    Function to validate a list of servicebus messages
    """

    class MessageInstances(BaseModel):

        """
        Represents a pydantic model for a list of ServiceUser instances.

        Args:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.

        Attributes:
            messagedata (list[ServiceUser]): The list of ServiceUser instances.
        """

        messagedata: list[model]

    try:
        MessageInstances(messagedata=data)
        print("Validation succeeded!")
        print(f"{len(data)} messages processed")
        return data
    except ValidationError as e:
        print(e)
        raise e
