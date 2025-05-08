import time
import logging
import csv
import wget
import os
import shutil
import sys
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)

def fetch_GLODAP_table():
    url = 'https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0283442/GLODAPv2.2023_Merged_Master_File.csv'
    filename = wget.download(url)

    if os.path.exists(filename):
        logging.info("Download successful!")
        shutil.copyfile("./GLODAPv2.2023_Merged_Master_File.csv", "/mnt/bucket/GLODAPv2.2023_Merged_Master_File.csv")

        input_file="/mnt/bucket/GLODAPv2.2023_Merged_Master_File.csv"
        output_file="/mnt/bucket/GLODAP_cleaned.csv"

        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile)
            writer = csv.writer(outfile, quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                try:    
                    # Check if the number of fields is 50
                    if len(row) == 109:
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


def load_GLODAP_table():

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;
        
        CREATE TABLE IF NOT EXISTS glodap (
            expocode VARCHAR(255),
            cruise VARCHAR(255),
            station VARCHAR(255),
            region VARCHAR(255),
            castnumber VARCHAR(255),
            year FLOAT,
            month FLOAT,
            day FLOAT,
            hour FLOAT,
            minute FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            bottomdepth FLOAT,
            maxsampdepth FLOAT,
            bottle FLOAT,
            pressure FLOAT,
            depth FLOAT,
            temperature FLOAT,
            theta FLOAT,
            salinity FLOAT,
            salinityf FLOAT,
            salinityqc FLOAT,
            sigma0 FLOAT,
            sigma1 FLOAT,
            sigma2 FLOAT,
            sigma3 FLOAT,
            sigma4 FLOAT,
            gamma FLOAT,
            oxygen FLOAT,
            oxygenf FLOAT,
            oxygenqc FLOAT,
            aou FLOAT,
            aouf FLOAT,
            nitrate FLOAT,
            nitratef FLOAT,
            nitrateqc FLOAT,
            nitrite FLOAT,
            nitritef FLOAT,
            silicate FLOAT,
            silicatef FLOAT,
            silicateqc FLOAT,
            phosphate FLOAT,
            phosphatef FLOAT,
            phosphateqc FLOAT,
            tco2 FLOAT,
            tco2f FLOAT,
            tco2qc FLOAT,
            talk FLOAT,
            talkf FLOAT,
            talkqc FLOAT,
            fco2 FLOAT,
            fco2f FLOAT,
            fco2temp FLOAT,
            phts25p0 FLOAT,
            phts25p0f FLOAT,
            phtsinsitutp FLOAT,
            phtsinsitutpf FLOAT,
            phtsqc FLOAT,
            cfc11 FLOAT,
            pcfc11 FLOAT,
            cfc11f FLOAT,
            cfc11qc FLOAT,
            cfc12 FLOAT,
            pcfc12 FLOAT,
            cfc12f FLOAT,
            cfc12qc FLOAT,
            cfc113 FLOAT,
            pcfc113 FLOAT,
            cfc113f FLOAT,
            cfc113qc FLOAT,
            ccl4 FLOAT,
            pccl4 FLOAT,
            ccl4f FLOAT,
            ccl4qc FLOAT,
            sf6 FLOAT,
            psf6 FLOAT,
            sf6f FLOAT,
            sf6qc FLOAT,
            c13 FLOAT,
            c13f FLOAT,
            c13qc FLOAT,
            c14 FLOAT,
            c14f FLOAT,
            c14err FLOAT,
            h3 FLOAT,
            h3f FLOAT,
            h3err FLOAT,
            he3 FLOAT,
            he3f FLOAT,
            he3err FLOAT,
            he FLOAT,
            hef FLOAT,
            heerr FLOAT,
            neon FLOAT,
            neonf FLOAT,
            neonerr FLOAT,
            o18 FLOAT,
            o18f FLOAT,
            toc FLOAT,
            tocf FLOAT,
            doc FLOAT,
            docf FLOAT,
            don FLOAT,
            donf FLOAT,
            tdn FLOAT,
            tdnf FLOAT,
            chla FLOAT,
            chlaf FLOAT,
            doi TEXT
        );

        TRUNCATE glodap;

        COPY glodap (
            expocode,
            cruise,
            station,
            region,
            castnumber,
            year,
            month,
            day,
            hour,
            minute,
            latitude,
            longitude,
            bottomdepth,
            maxsampdepth,
            bottle,
            pressure,
            depth,
            temperature,
            theta,
            salinity,
            salinityf,
            salinityqc,
            sigma0,
            sigma1,
            sigma2,
            sigma3,
            sigma4,
            gamma,
            oxygen,
            oxygenf,
            oxygenqc,
            aou,
            aouf,
            nitrate,
            nitratef,
            nitrateqc,
            nitrite,
            nitritef,
            silicate,
            silicatef,
            silicateqc,
            phosphate,
            phosphatef,
            phosphateqc,
            tco2,
            tco2f,
            tco2qc,
            talk,
            talkf,
            talkqc,
            fco2,
            fco2f,
            fco2temp,
            phts25p0,
            phts25p0f,
            phtsinsitutp,
            phtsinsitutpf,
            phtsqc,
            cfc11,
            pcfc11,
            cfc11f,
            cfc11qc,
            cfc12,
            pcfc12,
            cfc12f,
            cfc12qc,
            cfc113,
            pcfc113,
            cfc113f,
            cfc113qc,
            ccl4,
            pccl4,
            ccl4f,
            ccl4qc,
            sf6,
            psf6,
            sf6f,
            sf6qc,
            c13,
            c13f, 
            c13qc, 
            c14,
            c14f, 
            c14err, 
            h3, 
            h3f, 
            h3err, 
            he3, 
            he3f, 
            he3err, 
            he, 
            hef, 
            heerr, 
            neon,
            neonf,
            neonerr,
            o18,
            o18f, 
            toc,
            tocf, 
            doc,
            docf, 
            don,
            donf, 
            tdn,
            tdnf, 
            chla,
            chlaf, 
            doi
        )
        FROM '/mnt/bucket/GLODAP_cleaned.csv'
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
    os.remove("/mnt/bucket/GLODAPv2.2023_Merged_Master_File.csv")

def assign_GLODAP_hex():
    # revise for higher resolution in production
    sql_statements = """
        ALTER TABLE glodap ADD COLUMN IF NOT EXISTS location GEOMETRY(point, 4326);
        UPDATE glodap SET location = ST_SETSRID(ST_MakePoint(cast(longitude as float), cast(latitude as float)),4326);

        ALTER TABLE glodap ADD COLUMN IF NOT EXISTS hex_05 H3INDEX;
        UPDATE glodap SET hex_05 = H3_LAT_LNG_TO_CELL(location, 5);
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
    'dataset_ETL_GLODAP_obs',
    default_args=default_args,
    description='Fetch observations from GLODAP, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_clean_GLODAP_table = PythonOperator(
    task_id='fetch_clean_GLODAP_table',
    python_callable=fetch_GLODAP_table,
    provide_context=True,
    dag=dag
)

load_cleaned_GLODAP_table = PythonOperator(
    task_id='load_cleaned_GLODAP_table',
    python_callable=load_GLODAP_table,
    provide_context=True,
    dag=dag
)

assign_hexes_to_GLODAP= PythonOperator(
    task_id='assign_hexes_to_GLODAP',
    python_callable=assign_GLODAP_hex,
    provide_context=True,
    dag=dag
)

cleanup_nonocean_hexes = PostgresOperator(
    task_id='cleanup_nonocean_hexes',
    postgres_conn_id='oceexp-db',
    sql="""
    DELETE FROM glodap t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );
    """,
    dag=dag
)

assign_fks = PostgresOperator(
    task_id='assign_fks',
    postgres_conn_id='oceexp-db',
    sql="""
    ALTER TABLE glodap ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    """,
    dag=dag
)


# Define task dependencies
fetch_clean_GLODAP_table >> load_cleaned_GLODAP_table >> assign_hexes_to_GLODAP