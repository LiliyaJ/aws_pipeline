import os
import json
from transformation_helper import validate_json
from transformation_helper import transform_tasks_data
from transformation_helper import transform_keyword_info
from transformation_helper import transform_monthly_search_volume
from transformation_helper import transform_impressions_data

# Read the JSON from file
file_path = 'data/test_json.json'  # Replace with your actual file path
with open(file_path, 'r') as file:
    file_content = file.read()

# Validate the JSON content
if validate_json(file_content):
    print("The JSON is valid, no errors found.")
else:
    print("The JSON contains errors.")


# Parse the JSON content
data = json.loads(file_content)

# # Call your transformation function
# task_data = transform_tasks_data(data)
# print(f'TASK DATA: {task_data}')

keywords_data = transform_impressions_data(data)
print(json.dumps(keywords_data, indent=4))