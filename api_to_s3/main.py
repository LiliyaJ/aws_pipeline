import json
import os
import boto3
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

        # Save to S3 bucket
        bucket_name = "keywords-json-data"  # Ensure this bucket exists
        s3_key = "historical_search_volume.json"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json.dumps(combined_responses),
            ContentType="application/json"
        )

        print("Data successfully saved to S3.")
        return {"status": "success", "message": "Data saved to S3"}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {"status": "error", "message": str(e)}

#for local debugging
# if __name__ == "__main__":
#     # Simulated event
#     event = {
#         "keywords": ["data analyst", "analytics engineer", "data engineer"],
#         "locations_languages": {
#             "Germany": "German",
#             "Switzerland": "German",
#             "United States": "English"
#         },
#         "dfs_login": DFS_LOGIN,
#         "dfs_key": DFS_KEY
#     }

#     # Call the Lambda function
#     response = lambda_handler(event)
#     print(response)
