import pandas as pd
import boto3
from io import StringIO

from datetime import datetime
from sqlalchemy import create_engine

# Initialize S3 client (you can pass it as an argument if you need to use the same S3 client in other parts of your code)
s3_client = boto3.client('s3')

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

def save_to_s3(df, bucket_name, s3_key):
    """ Helper function to save a DataFrame as a CSV to S3 """
    # Convert DataFrame to CSV in-memory
    csv_buffer = pd.io.common.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload the CSV to the specified S3 bucket
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Successfully uploaded {s3_key} to {bucket_name}")
