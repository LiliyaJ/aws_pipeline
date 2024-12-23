import boto3
import json
import pandas as pd
from datetime import datetime, timedelta
import os  # To access environment variables

# Assuming you still have your transformation and load functions
from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data, validate_json
from load_helper import save_to_s3  # Import the save_to_s3 function

def main():
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # S3 Buckets
    input_s3_bucket = "us-keywords-json-data"
    output_s3_bucket = "transformed-json-data"  # New bucket to store transformed data
    s3_prefix = ""  # No subfolder, so prefix is empty

    # List all files in the S3 bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=input_s3_bucket, Prefix=s3_prefix)

    # Loop over the S3 files
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        print(f"Processing file: {file_key}")
        
        # Extract date from the filename (adjust split if necessary)
        file_date_str = file_key.split('_')[3]  # Adjust this part if your file naming convention differs

        # Read the JSON file from S3
        file_obj = s3_client.get_object(Bucket=input_s3_bucket, Key=file_key)
        json_data = json.loads(file_obj['Body'].read().decode('utf-8'))
        
        # Validate JSON
        if validate_json(json_data):
            # Perform transformations and assign each to a variable
            tasks_df = transform_tasks_data(json_data)
            print(f"Transformed tasks data: {tasks_df.head()}")
            
            keyword_info_df = transform_keyword_info(json_data)
            print(f"Transformed keyword info data: {keyword_info_df.head()}")
            
            monthly_search_volume_df = pd.DataFrame(transform_monthly_search_volume(json_data))
            print(f"Transformed monthly search volume data: {monthly_search_volume_df.head()}")
            
            impressions_df = transform_impressions_data(json_data)
            print(f"Transformed impressions data: {impressions_df.head()}")
            
            # Dynamically create CSV file names using the function names
            tasks_csv_key = f"tasks_{file_key.split('/')[-1].replace('.json', '_transform_tasks_data.csv')}"
            keyword_info_csv_key = f"keyword_info_{file_key.split('/')[-1].replace('.json', '_transform_keyword_info.csv')}"
            monthly_search_volume_csv_key = f"monthly_search_volume_{file_key.split('/')[-1].replace('.json', '_transform_monthly_search_volume.csv')}"
            impressions_csv_key = f"impressions_{file_key.split('/')[-1].replace('.json', '_transform_impressions_data.csv')}"

            # Save each DataFrame as a CSV in the output S3 bucket
            save_to_s3(tasks_df, output_s3_bucket, tasks_csv_key)
            save_to_s3(keyword_info_df, output_s3_bucket, keyword_info_csv_key)
            save_to_s3(monthly_search_volume_df, output_s3_bucket, monthly_search_volume_csv_key)
            save_to_s3(impressions_df, output_s3_bucket, impressions_csv_key)

# Run the main function
if __name__ == "__main__":
    main()
