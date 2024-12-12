import os
import json
from transformation_helper import validate_json, transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data


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
#print(json.dumps(keywords_data, indent=4))
print(keywords_data)