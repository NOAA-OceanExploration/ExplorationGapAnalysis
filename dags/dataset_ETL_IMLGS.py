import time
import logging
import csv
import wget
import os
import shutil
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)


def fetch_IMLGS_table():
    url = 'https://www.ngdc.noaa.gov/geosamples-api/api//samples/csv?order=facility_code%3Aasc&order=platform%3Aasc&order=cruise%3Aasc&order=sample%3Aasc'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        shutil.copyfile("./geosamples_export.csv", "/mnt/bucket/IMLGS.csv")

        input_file="/mnt/bucket/IMLGS.csv"
        output_file="/mnt/bucket/IMLGS_cleaned.csv"

        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                try:    
                    # Check if the number of fields is 50
                    if len(row) == 24:
                        # Create a new row with quotes around most fields, except for fields 22 and 23
                        processed_row = [
                            field for index, field in enumerate(row)
                        ]
                        # Write the processed row to the output file
                        writer.writerow(processed_row)
                except csv.Error as e:
                    # Handle the error: skip the row with the large field
                    logging.info(f"Skipping row due to error: {e}")
                    continue  # Skip the current row and proceed to the next one
        
        logging.info("Finished cleaning file, cleanup started...")

def load_IMLGS_table():

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;
        
        CREATE TABLE IF NOT EXISTS imlgs (
            repository VARCHAR,
            platform VARCHAR,
            cruiseID VARCHAR,
            sampleID VARCHAR,
            device VARCHAR,
            date VARCHAR,
            dateEnded VARCHAR,
            latitude FLOAT,
            endLatitude	FLOAT,
            longitude FLOAT,
            endLongitude FLOAT,
            depth FLOAT,
            endDepth FLOAT,
            storage	VARCHAR,
            coreLength_cm FLOAT,
            coreDiameter_cm FLOAT,
            principalInv VARCHAR,
            physiogProvince VARCHAR,
            lake VARCHAR,
            IGSN VARCHAR,
            altCruiseID VARCHAR,
            comments VARCHAR,
            NCEIurl VARCHAR,
            IMLGSnumber VARCHAR
        );

        TRUNCATE imlgs;

        COPY imlgs (
            repository, 
            platform,
            cruiseID,
            sampleID,
            device,
            date,
            dateEnded,
            latitude,
            endLatitude,
            longitude,
            endLongitude,
            depth,
            endDepth,
            storage,
            coreLength_cm,
            coreDiameter_cm,
            principalInv,
            physiogProvince,
            lake,
            IGSN,
            altCruiseID,
            comments,
            NCEIurl,
            IMLGSnumber
        )
        FROM '/mnt/bucket/IMLGS_cleaned.csv'
        WITH (FORMAT csv, HEADER true);
    """

    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

    logging.info("Cleaning up csv...")
    os.remove("/mnt/bucket/IMLGS.csv")

def assign_IMLGS_hex():
    # revise for higher resolution in production
    sql_statements = """
        ALTER TABLE imlgs ADD COLUMN IF NOT EXISTS location GEOMETRY(point, 4326);
        UPDATE imlgs SET location = ST_SETSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4326);

        ALTER TABLE imlgs ADD COLUMN IF NOT EXISTS hex_05 H3INDEX;
        UPDATE imlgs SET hex_05 = H3_LAT_LNG_TO_CELL(location, 5);
    """
     # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    try:
        logging.info("Executing SQL statements to assign hexes...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dataset_ETL_IMLGS',
    default_args=default_args,
    description='Fetch samples from IMLGS, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_clean_IMLGS_table = PythonOperator(
    task_id='fetch_clean_IMLGS_table',
    python_callable=fetch_IMLGS_table,
    provide_context=True,
    dag=dag
)

load_cleaned_IMLGS_table = PythonOperator(
    task_id='load_cleaned_IMLGS_table',
    python_callable=load_IMLGS_table,
    provide_context=True,
    dag=dag
)

assign_hexes_to_IMLGS= PythonOperator(
    task_id='assign_hexes_to_IMLGS',
    python_callable=assign_IMLGS_hex,
    provide_context=True,
    dag=dag
)
# Define task dependencies
fetch_clean_IMLGS_table >> load_cleaned_IMLGS_table >> assign_hexes_to_IMLGS