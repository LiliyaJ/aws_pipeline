import json

def validate_json(json_data):
    """
    Validate the JSON data to check if there is any error.

    Parameters:
    json_data (str or dict): JSON data to validate.

    Returns:
    bool: True if the JSON is valid (no errors found), False otherwise.
    """
    try:
        # If it's a string, load it into JSON format
        if isinstance(json_data, str):
            json_data = json.loads(json_data)

        # Check for errors in the JSON data
        for entry in json_data:
            if 'error' in entry:
                print(f"Error found: {entry['error']}")
                return False  # Return False if there's an error

        return True  # Return True if no errors found

    except json.JSONDecodeError as e:
        print(f"Invalid JSON format: {e}")
        return False


#1. Tasks Table:

def transform_tasks_data(data):
    tasks = []
    for task in data:
        task_info = task.get("tasks", [])
        
        for task_entry in task_info:
            task_record = {
                "task_id": task_entry.get("id"),
                "status_code": task_entry.get("status_code"),
                "status_message": task_entry.get("status_message"),
                "path": task_entry.get("path"),
                "location_name": task_entry.get("data", {}).get("location_name"),
                "language_name": task_entry.get("data", {}).get("language_name"),
                "keywords": task_entry.get("data", {}).get("keywords"),
                "api_function": task_entry.get("data", {}).get("function"),
                "cost": task_entry.get("cost"),
                "time": task_entry.get("time")
            }
            tasks.append(task_record)
    return tasks

#2. Keyword Info Table:
