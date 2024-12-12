import json
import boto3
import psycopg2
from datetime import datetime

def load_data_from_s3(bucket_name, file_key):
    """ Load raw data from S3 bucket """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return json.loads(response['Body'].read().decode('utf-8'))

def process_data(data):
    """ Process data using the transformation functions """
    from transform_functions import transform_impressions_data, transform_monthly_search_volume
    
    # Apply transformation functions
    transformed_impressions_data = transform_impressions_data(data)
    transformed_monthly_search_volume = transform_monthly_search_volume(data)

    return transformed_impressions_data, transformed_monthly_search_volume

def write_to_redshift(data, redshift_table, redshift_connection):
    """ Write transformed data into Redshift """
    conn = psycopg2.connect(redshift_connection)
    cursor = conn.cursor()
    
    for entry in data:
        columns = ', '.join(entry.keys())
        values = ', '.join([f"'{v}'" for v in entry.values()])
        
        insert_query = f"INSERT INTO {redshift_table} ({columns}) VALUES ({values})"
        
        try:
            cursor.execute(insert_query)
            conn.commit()
        except Exception as e:
            print(f"Error inserting data into Redshift: {e}")
            conn.rollback()
    
    cursor.close()
    conn.close()
