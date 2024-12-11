import json
import os
import boto3
import uuid
from datetime import datetime
from helper import fetch_historical_search_volume

DFS_LOGIN = os.getenv('DFS_LOGIN')
DFS_KEY = os.getenv('DFS_KEY')

# S3 Client for testing
s3_client = boto3.client("s3")

def lambda_handler(event, context=None):
    """
    Simulate Lambda function to fetch historical search volume data and save it to S3.
    """
    try:
        # Parse the simulated event
        keywords = event["keywords"]
        locations_languages = event["locations_languages"]
        dfs_login = event["dfs_login"]
        dfs_key = event["dfs_key"]

        # Fetch data using helper function
        combined_responses = fetch_historical_search_volume(keywords, locations_languages, dfs_login, dfs_key)

        # Save each response into a separate JSON file in S3
        bucket_name = "us-keywords-json-data"  # Ensure this bucket exists
        unique_id = str(uuid.uuid4())  # Generate a unique identifier
        current_date = datetime.now().strftime("%Y-%m-%d")  # Get current date
        s3_key = f"historical_search_volume_{current_date}_{unique_id}.json"  # Unique file name

        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(combined_responses),
            ContentType="application/json"
        )

        print(f"Data successfully saved to S3 as {s3_key}.")
        return {"status": "success", "message": f"Data saved to S3 as {s3_key}"}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}

#for local debugging
if __name__ == "__main__":
    # Simulated event
    event = {
        "keywords": ["data analyst", "analytics engineer", "data engineer"],
        "locations_languages": {
             "United Kingdom": "English",
            "Finland": "Finnish",
            "Denmark": "Danish"
        },
        "dfs_login": DFS_LOGIN,
        "dfs_key": DFS_KEY
    }

    # Call the Lambda function
    response = lambda_handler(event)
    print(response)
