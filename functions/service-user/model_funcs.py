"""
Module containing some useful functions to work with dictionaries if needed
"""


def is_valid_array_of_objects(value: list[dict[str, any]], key: str) -> None:
    """
    Check if a value is a valid array of objects.

    Args:
    value: The value to check.
    key: The key associated with the value.

    Raises:
    ValueError: If the value is not a list or contains items that are not dictionaries.
    """

    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise ValueError(f"Invalid JSON object: {key} is not a list of objects.")


def is_valid_object(value: dict[str, any], key: str) -> None:
    """
    Check if a value is a valid object.

    Args:
        value: The value to check.
        key: The key associated with the value.

    Raises:
        ValueError: If the value is not a dictionary.
    """

    if not isinstance(value, dict):
        raise ValueError(f"Invalid JSON object: {key} is not a dictionary.")


def process_properties(
    input_dictionary: dict, schema: dict, remove: bool = False
) -> dict:
    """
    Process properties in an input dictionary according to a schema.

    Args:
        input_dictionary (dict): The input dictionary to process.
        schema: The schema containing the properties definition.
        remove (bool, optional): Whether to remove undefined properties. Defaults to False.

    Returns:
        dict: The processed input dictionary.

    Raises:
        ValueError: If the input dictionary or its nested objects/lists do not match the expected structure defined in the schema.
    """

    properties = schema["properties"]
    property_keys = set(properties)

    if remove:
        # Remove all undefined properties.
        for key in set(input_dictionary).difference(property_keys):
            del input_dictionary[key]
    else:
        # Fill all missing properties.
        for key in property_keys.difference(input_dictionary):
            value = properties[key]
            input_dictionary[key] = (
                []
                if value["type"] == "array"
                else {}
                if value["type"] == "object"
                else "FIELD NOT IN MESSAGE DATA"
            )

    # Recurse inside all properties.
    for key, value in properties.items():
        if key not in input_dictionary:
            continue

        if value["type"] == "array" and value["items"]["type"] == "object":
            is_valid_array_of_objects(input_dictionary[key], key)
            for item in input_dictionary[key]:
                process_properties(item, value["items"], remove)

        elif value["type"] == "object":
            is_valid_object(input_dictionary[key], key)
            process_properties(input_dictionary[key], value, remove)

    return input_dictionary


def fill_nonexistent_properties(input_dictionary: dict, schema: dict) -> dict:
    """
    Fill missing properties in an input dictionary according to a schema.

    Args:
        input_dictionary (dict): The input dictionary to fill missing properties in.
        schema: The schema containing the properties definition.

    Returns:
        dict: The input dictionary with missing properties filled.

    Raises:
        ValueError: If the input dictionary or its nested objects/lists do not match the expected structure defined in the schema.
    """

    return process_properties(input_dictionary, schema)


def remove_undefined_properties(input_dictionary: dict, schema: dict) -> dict:
    """
    Remove properties in an input dictionary that are not defined in the schema.

    Args:
        input_dictionary (dict): The input dictionary to remove undefined properties from.
        schema: The schema containing the properties definition.

    Returns:
        dict: The input dictionary with undefined properties removed.

    Raises:
        ValueError: If the input dictionary or its nested objects/lists do not match the expected structure defined in the schema.
    """

    return process_properties(input_dictionary, schema, remove=True)


def convert_to_lower(data: dict) -> dict:
    """
    Convert dictionary keys to lowercase.

    Args:
        data (dict): The dictionary object to convert.

    Returns:
        dict: The dictionary with all keys converted to lowercase.
    """

    return {k.lower(): v for k, v in data.items()}