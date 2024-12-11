import json
import os
import boto3
from helper import fetch_historical_search_volume

DFS_LOGIN = os.getenv('DFS_LOGIN')
DFS_KEY = os.getenv('DFS_KEY')

# Initialize S3 client
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    """
    AWS Lambda handler to fetch historical search volume data and save it to S3.

    Parameters:
        event (dict): The event triggering the Lambda function. Expects a JSON body with:
                      - keywords: List of keywords to fetch data for.
                      - locations_languages: Dictionary mapping locations to language names.
                      - dfs_login: DataForSEO API login.
                      - dfs_key: DataForSEO API key.

    Returns:
        dict: Response message with status.
    """
    try:
        # Parse event body
        body = json.loads(event["body"])
        keywords = body["keywords"]
        locations_languages = body["locations_languages"]
        dfs_login = body["dfs_login"]
        dfs_key = body["dfs_key"]

        # Fetch historical search volume data
        combined_responses = fetch_historical_search_volume(keywords, locations_languages, dfs_login, dfs_key)

        # Save response to S3
        bucket_name = "keywords-json-data"  # Change this to your bucket name
        s3_key = "historical_search_volume.json"  # The name of the file in S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(combined_responses),
            ContentType="application/json"
        )

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data successfully saved to S3"})
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }