def fill_nonexistent_properties(input_dictionary, schema):
    """
    Fill missing properties in input_dictionary according to the schema.
    """
    properties = schema['properties']
    missing_properties = set(properties).difference(input_dictionary)

    # Fill all missing properties.
    for key in missing_properties:
        value = properties[key]
        if value['type'] == 'array':
            input_dictionary[key] = []
        elif value['type'] == 'object':
            input_dictionary[key] = {}
        else:
            input_dictionary[key] = "FIELD NOT IN MESSAGE DATA"
    
    # Recurse inside all properties.
    for key, value in properties.items():
        
        # If it's an array of objects, recurse inside each item.
        if value['type'] == 'array' and value['items']['type'] == 'object':
            object_list = input_dictionary[key]

            if not isinstance(object_list, list):
                raise ValueError(
                    f"Invalid JSON object: {key} is not a list.")

            for item in object_list:
                if not isinstance(item, dict):
                    raise ValueError(
                        f"Invalid JSON object: {key} is not a list of objects.")
                fill_nonexistent_properties(item, value['items'])

        # If it's an object, recurse inside it.
        elif value['type'] == 'object':
            obj = input_dictionary[key]
            if not isinstance(obj, dict):
                raise ValueError(
                    f"Invalid JSON object: {key} is not a dictionary.")
            fill_nonexistent_properties(obj, value)

    return input_dictionary

def remove_undefined_properties(input_dictionary, schema):
    """
    Remove properties in input_dictionary that are not defined in the schema.
    """
    properties = schema['properties']
    undefined_properties = set(input_dictionary).difference(properties)

    # Remove all undefined properties.
    for key in undefined_properties:
        del input_dictionary[key]
    
    # Recurse inside all existing sproperties.
    for key, value in input_dictionary.items():
        property_schema = properties[key]

        # If it's an array of objects, recurse inside each item.
        if isinstance(value, list):
            if property_schema['type'] != 'array':
                raise ValueError(
                    f"Invalid JSON object: {key} is not a list.")

            # We're only dealing with objects inside arrays.
            if property_schema['items']['type'] != 'object':
                continue
            
            for item in value:
                # Make sure each item is an object.
                if not isinstance(item, dict):
                    raise ValueError(
                        f"Invalid JSON object: {key} is not a list of objects.")
                remove_undefined_properties(item, property_schema['items'])
        
        # If it's an object, recurse inside it.
        elif isinstance(value, dict):
            # Make sure the object is supposed to be an object.
            if property_schema['type'] != 'object':
                raise ValueError(
                    f"Invalid JSON object: {key} is not an object.")

            remove_undefined_properties(value, property_schema)

    return input_dictionary


# convert input data dicionary keys to lowercase for comparison with model
def convert_to_lower(data: dict):
    return {k.lower() if isinstance(k, str) else k: v for k, v in data.items()}
