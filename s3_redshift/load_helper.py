import json
import boto3
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine
from transformation_helper import transform_tasks_data, transform_keyword_info, transform_monthly_search_volume, transform_impressions_data


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

def get_redshift_credentials(secret_name):
    """ Retrieve Redshift credentials from AWS Secrets Manager """
    # Create a boto3 session and Secrets Manager client
    client = boto3.client('secretsmanager', region_name='us-west-2')  # Replace with your region
    try:
        # Retrieve the secret from Secrets Manager
        response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise