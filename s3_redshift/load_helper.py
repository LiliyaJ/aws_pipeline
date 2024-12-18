import json
import boto3
import psycopg2
from datetime import datetime
from sqlalchemy import create_engine


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
