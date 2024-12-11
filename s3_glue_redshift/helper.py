import json

def validate_json(json_data):
    """
    Validates if the provided JSON is correctly formatted.
    Returns a tuple (is_valid, message) where:
    - is_valid (bool): True if the JSON is valid, False otherwise
    - message (str): Error message if invalid or success message if valid
    """
    try:
        # Try to parse the JSON data
        json.loads(json_data)
        return True, "Valid JSON"
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
