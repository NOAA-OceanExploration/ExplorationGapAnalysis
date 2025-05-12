import os
import wget
import zipfile
import pyobis
import time
import logging
import csv
import sys
from pyobis import occurrences as occ
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

csv.field_size_limit(sys.maxsize)

def fetch_OBIS_table():

    if os.path.exists(f"/mnt/bucket/output.csv"): # temporary step for dev
        print('file exists, skipping download')
    else: 
        # get OBIS data for DNA sequences and depths >200m
        obisdata = occ.search(hasextensions="DNADerivedData", startdepth=200, enddepth=12000).execute()
        obisdata.to_csv('/mnt/bucket/output.csv', index=False)

    logging.info(f"Looking for /mnt/bucket/output.csv...")
    if os.path.exists("/mnt/bucket/output.csv"):
        logging.info("Download successful!")

        # Input and output file paths
        input_file = "/mnt/bucket/output.csv"
        output_file = "/mnt/bucket/cleaned_NR151.csv"

        # Check if the file exists before processing
        if not os.path.exists(input_file):
            logging.info(f"Input file {input_file} not found.")
            #raise Exception(f"Input file {input_file} not found.")
        
        with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
            open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            
            # Create a CSV reader and writer
            reader = csv.reader(infile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer = csv.writer(outfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            # Process each row
            for row in reader:
                try:    
                    # Check if the number of fields is 50
                    #print(f"Row length: {len(row)}")  # Debugging
                    if len(row) == 151:
                        # Create a new row with quotes around most fields, except for fields 22 and 23
                        processed_row = [
                            field  
                            #f'"{field}"' if index not in [21, 22] else field  # Field 22 is index 21, field 23 is index 22
                            for index, field in enumerate(row)
                        ]
                        # Write the processed row to the output file
                        writer.writerow(processed_row)
                except csv.Error as e:
                    # Handle the error: skip the row with the large field
                    logging.info(f"Skipping row due to error: {e}")
                    continue  # Skip the current row and proceed to the next one
        
        logging.info("Finished cleaning file, cleanup started...")

        os.remove(f"/mnt/bucket/output.csv")
        #os.remove(f"/mnt/bucket/{occdatakey}.zip")


def load_OBIS_table_csv():
    sql_statements = """
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;

        DROP TABLE IF EXISTS obis_sequences;
        
        CREATE TABLE obis_sequences (
            basisOfRecord TEXT,
            bibliographicCitation TEXT,
            brackish TEXT,
            catalogNumber TEXT,
            class TEXT,
            classid TEXT,
            collectionCode TEXT,
            coordinateUncertaintyInMeters TEXT,
            country TEXT,
            date_end TEXT,
            date_mid TEXT,
            date_start TEXT,
            date_year TEXT,
            decimalLatitude FLOAT,
            decimalLongitude FLOAT,
            depth FLOAT,
            eventDate TEXT,
            eventID TEXT,
            footprintWKT GEOMETRY,
            identificationReferences TEXT,
            identificationRemarks TEXT,
            infrakingdom TEXT,
            infrakingdomid TEXT,
            infraphylum TEXT,
            infraphylumid TEXT,
            institutionCode TEXT,
            kingdom TEXT,
            kingdomid TEXT,
            locality TEXT,
            marine TEXT,
            materialSampleID TEXT,
            maximumDepthInMeters FLOAT,
            minimumDepthInMeters FLOAT,
            modified TEXT,
            occurrenceID TEXT,
            occurrenceStatus TEXT,
            "order" TEXT,
            orderid TEXT,
            organismQuantity TEXT,
            organismQuantityType TEXT,
            phylum TEXT,
            phylumid TEXT,
            recordedBy TEXT,
            sampleSizeUnit TEXT,
            sampleSizeValue TEXT,
            samplingProtocol TEXT,
            scientificName TEXT,
            scientificNameID TEXT,
            subkingdom TEXT,
            subkingdomid TEXT,
            subphylum TEXT,
            subphylumid TEXT,
            taxonRank TEXT,
            id UUID PRIMARY KEY,
            dataset_id TEXT,
            node_id TEXT,
            dropped TEXT,
            absence TEXT,
            originalScientificName TEXT,
            aphiaID TEXT,
            flags TEXT,
            bathymetry TEXT,
            shoredistance TEXT,
            sst TEXT,
            sss TEXT,
            family TEXT,
            familyid TEXT,
            genus TEXT,
            genusid TEXT,
            species TEXT,
            speciesid TEXT,
            specificEpithet TEXT,
            subfamily TEXT,
            subfamilyid TEXT,
            subgenus TEXT,
            subgenusid TEXT,
            tribe TEXT,
            tribeid TEXT,
            suborder TEXT,
            suborderid TEXT,
            superfamily TEXT,
            superfamilyid TEXT,
            associatedOccurrences TEXT,
            associatedSequences TEXT,
            datasetID TEXT,
            geodeticDatum TEXT,
            locationID TEXT,
            nameAccordingTo TEXT,
            parentEventID TEXT,
            taxonID TEXT,
            verbatimIdentification TEXT,
            waterBody TEXT,
            subclass TEXT,
            subclassid TEXT,
            infraclass TEXT,
            infraclassid TEXT,
            superclass TEXT,
            superclassid TEXT,
            superorder TEXT,
            superorderid TEXT,
            datasetName TEXT,
            day TEXT,
            endDayOfYear TEXT,
            eventTime TEXT,
            license TEXT,
            maximumDistanceAboveSurfaceInMeters TEXT,
            minimumDistanceAboveSurfaceInMeters TEXT,
            month TEXT,
            recordNumber TEXT,
            startDayOfYear TEXT,
            year TEXT,
            unaccepted TEXT,
            scientificNameAuthorship TEXT,
            parvphylum TEXT,
            parvphylumid TEXT,
            megaclass TEXT,
            megaclassid TEXT,
            subterclass TEXT,
            subterclassid TEXT,
            associatedReferences TEXT,
            infraorder TEXT,
            infraorderid TEXT,
            ownerInstitutionCode TEXT,
            stateProvince TEXT,
            gigaclass TEXT,
            gigaclassid TEXT,
            collectionID TEXT,
            continent TEXT,
            countryCode TEXT,
            fieldNumber TEXT,
            identifiedBy TEXT,
            identifiedByID TEXT,
            individualCount TEXT,
            institutionID TEXT,
            occurrenceRemarks TEXT,
            preparations TEXT,
            recordedByID TEXT,
            type TEXT,
            verbatimDepth TEXT,
            dateIdentified TEXT,
            lifeStage TEXT,
            sex TEXT,
            parvorder TEXT,
            parvorderid TEXT,
            section TEXT,
            sectionid TEXT,
            subsection TEXT,
            subsectionid TEXT,
            identificationQualifier TEXT,
            subspecies TEXT,
            subspeciesid TEXT
        );



        COPY obis_sequences (
            basisOfRecord,
            bibliographicCitation,
            brackish,
            catalogNumber,
            class,
            classid,
            collectionCode,
            coordinateUncertaintyInMeters,
            country,
            date_end,
            date_mid,
            date_start,
            date_year,
            decimalLatitude,
            decimalLongitude,
            depth,
            eventDate,
            eventID,
            footprintWKT,
            identificationReferences,
            identificationRemarks,
            infrakingdom,
            infrakingdomid,
            infraphylum,
            infraphylumid,
            institutionCode,
            kingdom,
            kingdomid,
            locality,
            marine,
            materialSampleID,
            maximumDepthInMeters,
            minimumDepthInMeters,
            modified,
            occurrenceID,
            occurrenceStatus,
            "order",
            orderid,
            organismQuantity,
            organismQuantityType,
            phylum,
            phylumid,
            recordedBy,
            sampleSizeUnit,
            sampleSizeValue,
            samplingProtocol,
            scientificName,
            scientificNameID,
            subkingdom,
            subkingdomid,
            subphylum,
            subphylumid,
            taxonRank,
            id,
            dataset_id,
            node_id,
            dropped,
            absence,
            originalScientificName,
            aphiaID,
            flags,
            bathymetry,
            shoredistance,
            sst,
            sss,
            family,
            familyid,
            genus,
            genusid,
            species,
            speciesid,
            specificEpithet,
            subfamily,
            subfamilyid,
            subgenus,
            subgenusid,
            tribe,
            tribeid,
            suborder,
            suborderid,
            superfamily,
            superfamilyid,
            associatedOccurrences,
            associatedSequences,
            datasetID,
            geodeticDatum,
            locationID,
            nameAccordingTo,
            parentEventID,
            taxonID,
            verbatimIdentification,
            waterBody,
            subclass,
            subclassid,
            infraclass,
            infraclassid,
            superclass,
            superclassid,
            superorder,
            superorderid,
            datasetName,
            day,
            endDayOfYear,
            eventTime,
            license,
            maximumDistanceAboveSurfaceInMeters,
            minimumDistanceAboveSurfaceInMeters,
            month,
            recordNumber,
            startDayOfYear,
            year,
            unaccepted,
            scientificNameAuthorship,
            parvphylum,
            parvphylumid,
            megaclass,
            megaclassid,
            subterclass,
            subterclassid,
            associatedReferences,
            infraorder,
            infraorderid,
            ownerInstitutionCode,
            stateProvince,
            gigaclass,
            gigaclassid,
            collectionID,
            continent,
            countryCode,
            fieldNumber,
            identifiedBy,
            identifiedByID,
            individualCount,
            institutionID,
            occurrenceRemarks,
            preparations,
            recordedByID,
            type,
            verbatimDepth,
            dateIdentified,
            lifeStage,
            sex,
            parvorder,
            parvorderid,
            section,
            sectionid,
            subsection,
            subsectionid,
            identificationQualifier,
            subspecies,
            subspeciesid
        )
        FROM '/mnt/bucket/cleaned_NR151.csv'
        WITH (FORMAT csv, HEADER true, QUOTE '"'); 
    """

    # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    try:
        logging.info("Executing SQL statements...")
        pg_hook.run(sql_statements, autocommit=True)
        logging.info("SQL execution completed successfully!")
    except Exception as e:
        logging.error(f"SQL execution failed: {e}")
        raise

    #logging.info("Cleaning up csv...")
    #os.remove("/mnt/bucket/cleaned_NR50.csv")

def assign_OBIS_hex():
    # revise for higher resolution in production
    sql_statements = """
        ALTER TABLE obis_sequences ADD COLUMN location GEOMETRY(point, 4326);
        UPDATE obis_sequences SET location = ST_SETSRID(ST_MakePoint(cast(decimallongitude as float), cast(decimallatitude as float)),4326);

        ALTER TABLE obis_sequences ADD COLUMN hex_05 H3INDEX;
        UPDATE obis_sequences SET hex_05 = H3_LAT_LNG_TO_CELL(location, 5);
    """
     # Initialize PostgresHook
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")

    try:
        logging.info("Executing SQL statements...")
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
    'dataset_ETL_OBIS_sequences',
    default_args=default_args,
    description='Fetch sequences from OBIS, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_OBIS_query_table = PythonOperator(
    task_id='fetch_OBIS_query_table',
    python_callable=fetch_OBIS_table,
    provide_context=True,
    dag=dag
)

load_OBIS_table = PythonOperator(
    task_id='load_OBIS_table',
    python_callable=load_OBIS_table_csv,
    provide_context=True,
    dag=dag
)

assign_hexes_to_OBIS = PythonOperator(
    task_id='assign_hexes_to_OBIS',
    python_callable=assign_OBIS_hex,
    provide_context=True,
    dag=dag
)

# Define task dependencies
fetch_OBIS_query_table >> load_OBIS_table >> assign_hexes_to_OBIS
