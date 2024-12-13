import boto3
import json
import pandas as pd
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data, validate_json
from load_helper import write_to_redshift

def main():
    # Initialize GlueContext
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    
    # Initialize the job
    job.init('json-transformation-job', getResolvedOptions())

    # S3 Bucket and Path
    s3_bucket = "us-keywords-json-data"
    s3_prefix = "path/to/json/files/"  # Modify as needed

    # Set the date range for files older than one week
    one_week_ago = datetime.now() - timedelta(weeks=1)
    one_week_ago_str = one_week_ago.strftime('%Y-%m-%d')

    # Initialize S3 client
    s3_client = boto3.client('s3')

    # List all files in the S3 bucket with the specified prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

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

                # Convert to Glue DynamicFrames
                tasks_dynamic_frame = DynamicFrame.from_pandas(tasks_df, glueContext)
                keyword_info_dynamic_frame = DynamicFrame.from_pandas(keyword_info_df, glueContext)
                monthly_search_volume_dynamic_frame = DynamicFrame.from_pandas(monthly_search_volume_df, glueContext)
                impressions_dynamic_frame = DynamicFrame.from_pandas(impressions_df, glueContext)

                # Write to Redshift (assuming you have the function implemented in load_helper.py)
                write_to_redshift(tasks_dynamic_frame, 'tasks_table', 'playground', 'playground')
                write_to_redshift(keyword_info_dynamic_frame, 'keyword_info_table', 'playground', 'playground')
                write_to_redshift(monthly_search_volume_dynamic_frame, 'monthly_search_volume_table', 'playground', 'playground')
                write_to_redshift(impressions_dynamic_frame, 'impressions_table', 'playground', 'playground')
    
    # Commit the job
    job.commit()

# Run the main function
if __name__ == "__main__":
    main()
