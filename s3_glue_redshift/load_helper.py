import json
import boto3
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data


def load_data_from_s3(bucket_name, file_key):
    """ Load raw data from S3 bucket """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def process_data(data):
    """ Process data using the transformation functions """
    
    # Apply transformation functions
    transformed_tasks_data = transform_tasks_data(data)
    transformed_keyword_info = transform_keyword_info(data)   
    transformed_impressions_data = transform_impressions_data(data)
    transformed_monthly_search_volume = transform_monthly_search_volume(data)

    return transformed_tasks_data, transformed_keyword_info, transformed_impressions_data, transformed_monthly_search_volume

def write_to_redshift(df, redshift_table, redshift_connection):
    """ Append DataFrame data into Redshift """
    
    # Create SQLAlchemy engine for Redshift
    engine = create_engine(f'postgresql+psycopg2://{redshift_connection}')
    
    try:
        # Append DataFrame to Redshift using pandas to_sql method with if_exists='append'
        df.to_sql(redshift_table, con=engine, index=False, if_exists='append', method='multi')
        print(f"Data successfully inserted into {redshift_table}")
    except Exception as e:
        print(f"Error inserting data into Redshift: {e}")