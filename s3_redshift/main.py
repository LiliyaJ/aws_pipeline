import boto3
import sys
import json
import pandas as pd
from datetime import datetime, timedelta
import os  # To access environment variables

# Assuming you still have your transformation and load functions
from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data, validate_json
from load_helper import write_to_redshift

def main():
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # S3 Bucket and Path
    s3_bucket = "us-keywords-json-data"
    s3_prefix = ""  # No subfolder, so prefix is empty
    
    # Set the date range for files older than one week
    one_week_ago = datetime.now() - timedelta(weeks=1)
    one_week_ago_str = one_week_ago.strftime('%Y-%m-%d')

    # List all files in the S3 bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

    # Read Redshift credentials from environment variables
    redshift_host = os.environ['REDSHIFT_HOST']
    redshift_port = os.environ['REDSHIFT_PORT']
    redshift_dbname = os.environ['REDSHIFT_DBNAME']
    redshift_username = os.environ['REDSHIFT_USERNAME']
    redshift_password = os.environ['REDSHIFT_PASSWORD']

    # Construct Redshift connection string
    redshift_connection = f"{redshift_username}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_dbname}"

    # Loop over the S3 files
    for obj in response.get('Contents', []):
        file_key = obj['Key']
        file_date_str = file_key.split('_')[2]  # Extract date from filename, adjust split if needed

        if file_date_str <= one_week_ago_str:
            # Read the JSON file from S3
            file_obj = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
            json_data = json.loads(file_obj['Body'].read().decode('utf-8'))

            # Validate JSON
            if validate_json(json_data):
                # Transform the data
                tasks_df = pd.DataFrame(transform_tasks_data(json_data))
                keyword_info_df = pd.DataFrame(transform_keyword_info(json_data))
                monthly_search_volume_df = pd.DataFrame(transform_monthly_search_volume(json_data))
                impressions_df = pd.DataFrame(transform_impressions_data(json_data))

                # Write to Redshift
                write_to_redshift(tasks_df, 'tasks_table', redshift_connection)
                write_to_redshift(keyword_info_df, 'keyword_info_table', redshift_connection)
                write_to_redshift(monthly_search_volume_df, 'monthly_search_volume_table', redshift_connection)
                write_to_redshift(impressions_df, 'impressions_table', redshift_connection)

