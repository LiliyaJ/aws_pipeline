import psycopg2
from psycopg2 import sql
import redshift_connector
import os

# Read Redshift credentials from environment variables
redshift_host = os.environ['REDSHIFT_HOST']
redshift_dbname = os.environ['REDSHIFT_DBNAME']
redshift_port = os.environ['REDSHIFT_PORT']
redshift_username = os.environ['REDSHIFT_USERNAME']
redshift_password = os.environ['REDSHIFT_PASSWORD']
# Connection string for Redshift
connection_string = f"postgresql+psycopg2://{redshift_username}:{redshift_password}@{redshift_host}:{redshift_port}/{redshift_dbname}"

# Print out the connection string to check if everything is correct
print("Redshift connection string:", connection_string)

# Attempt to connect to Redshift
try:
    # Check if all the data we need is correct
    print("Checking Redshift credentials...")
    print(f"Username: {redshift_username}")
    print(f"Host: {redshift_host}")
    print(f"Port: {redshift_port}")
    print(f"Database Name: {redshift_dbname}")

    # Test the connection with psycopg2
    print("Attempting to connect to Redshift...")
    conn = redshift_connector.connect(
        database=redshift_dbname,
        user=redshift_username,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    print("Connection successful!")

    # Print connection details
    print("Connection details:", conn.get_dsn_parameters())

    # Example: Closing the connection
    conn.close()
except Exception as e:
    print(f"Error connecting to Redshift: {e}")