import json
import os
import boto3
import uuid
from datetime import datetime
from helper import fetch_historical_search_volume

# Environment variables for AWS Lambda
DFS_LOGIN = os.getenv('DFS_LOGIN')
DFS_KEY = os.getenv('DFS_KEY')

# S3 Client
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    """
    Lambda function to fetch historical search volume data and save each result into a unique JSON file in S3.
    """
    try:
        # Parse input from the event
        keywords = event["keywords"]
        locations_languages = event["locations_languages"]

        # Fetch data using the helper function
        combined_responses = fetch_historical_search_volume(keywords, locations_languages, DFS_LOGIN, DFS_KEY)

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
