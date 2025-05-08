from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import zipfile
import os
import geopandas as gpd
import h3
import csv
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def download_osm_water_polygons(url, output_folder):
    """Downloads and extracts OSM water polygons zip file."""
    zip_filename = os.path.join(output_folder, "water-polygons.zip")
    extracted_folder = os.path.join(output_folder, "water-polygons")
    
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    print("Downloading OSM water polygons...")
    response = requests.get(url, stream=True)
    
    # Check for successful response
    if response.status_code == 200:
        total_size = int(response.headers.get('Content-Length', 0))  # Get total file size
        with open(zip_filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print("Download complete.")
    else:
        print("Failed to download the file.")
        return None, None
    
    # Extract the ZIP file
    print("Extracting files...")
    with zipfile.ZipFile(zip_filename, "r") as zip_ref:
        zip_ref.extractall(extracted_folder)
    print(f"Files extracted to {extracted_folder}")

    return zip_filename, extracted_folder

def process_and_identify_hexagons(extracted_folder, output_csv):
    """Loads the shapefile, processes geometry, and saves hexagons to CSV."""
    shapefile_path = os.path.join(extracted_folder, "water-polygons/water-polygons-split-3857", "water_polygons.shp") # this could be more efficiently named/handled
    
    # Load the shapefile into a GeoDataFrame
    print("Loading shapefiles...")
    #print(shapefile_path)
    gdf = gpd.read_file(shapefile_path)
    print("...done")
    
    # Reproject to WGS 84 if the CRS isn't already EPSG:4326
    if gdf.crs != 'EPSG:4326':
        gdf = gdf.to_crs(epsg=4326)
        #print("Reprojected GeoDataFrame to EPSG:4326")

    # Identify water hexagons and save to CSV
    waterhexes = set()  # Initialize an empty set to store unique hexes
    for geom in gdf.geometry:
        geojson = geom.__geo_interface__  # Convert the geometry to GeoJSON format
        h3shp = h3.geo_to_h3shape(geojson)
        hexes = h3.polygon_to_cells_experimental(h3shp,5,'overlap')
        #hexes = h3.geo_to_cells(geojson, 4)  # Adjust resolution as needed
        waterhexes.update(hexes)  # Update the waterhexes set with the result
    
    # Write identified water hexagons to CSV
    with open(output_csv, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["h3_index"])  # Header row
        for hexagon in waterhexes:
            writer.writerow([hexagon])  # Write each hexagon index
    print(f"Water hexagons saved to {output_csv}")

# Function to load H3 data to PostgreSQL
def load_h3_to_postgis(csv_path, table_name, postgres_conn_id):
    pg_hook = PostgresHook(postgres_conn_id)
    engine = create_engine(pg_hook.get_uri())

    df = pd.read_csv(csv_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

# Define the DAG
dag = DAG(
    'dataset_create_hex_heirarchy_from_water_polygons',
    default_args=default_args,
    description='DAG to download, process, and save OSM water hexagons',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

# Define task arguments
WATER_POLYGON_URL = "https://osmdata.openstreetmap.de/download/water-polygons-split-3857.zip"
OUTPUT_FOLDER = "/tmp/osm_water_data"
OUTPUT_CSV = "/tmp/water_hexagons.csv"

# Task 1: Download and unzip OSM water polygons
download_task = PythonOperator(
    task_id='download_osm_water_polygons',
    python_callable=download_osm_water_polygons,
    op_kwargs={'url': WATER_POLYGON_URL, 'output_folder': OUTPUT_FOLDER},
    dag=dag,
)

# Task 2: Process shapefile and identify hexagons
process_task = PythonOperator(
    task_id='process_and_identify_hexagons',
    python_callable=process_and_identify_hexagons,
    op_kwargs={'extracted_folder': OUTPUT_FOLDER, 'output_csv': OUTPUT_CSV},
    dag=dag,
)

load_h3_task = PythonOperator(
    task_id='load_h3_to_postgis',
    python_callable=load_h3_to_postgis,
    op_kwargs={
        'csv_path': OUTPUT_CSV,
        'table_name': 'hex_ocean_polys_05',
        'postgres_conn_id': 'oceexp-db',
    },
    dag=dag,
)

# Task: Create the h3_children table
create_h3_primary = PostgresOperator(
    task_id='create_h3_primary',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        CREATE EXTENSION IF NOT EXISTS h3;
        CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;
        
        DROP TABLE IF EXISTS h3_oceans;

        CREATE TABLE h3_oceans AS 
        TABLE hex_ocean_polys_05;

        ALTER TABLE h3_oceans
        RENAME COLUMN h3_index TO hex_05;

        ALTER TABLE h3_oceans
        ALTER COLUMN hex_05 TYPE H3INDEX
        USING hex_05::H3INDEX;

        ALTER TABLE h3_oceans
        ADD PRIMARY KEY (hex_05);

        DROP TABLE IF EXISTS hex_ocean_polys_05; 
    """,
)

#        CREATE TABLE h3_oceans AS
#        SELECT
#            hex_05
#        FROM
#            hex_ocean_polys_04,
#            LATERAL H3_Cell_to_Children(CAST("h3_index" AS H3Index), 5) AS hex_05;

#        ALTER TABLE h3_oceans
#        ADD PRIMARY KEY (hex_05);

#        DROP TABLE IF EXISTS hex_ocean_polys_04; 


# Task: Create the h3_children table
create_h3_lineage = PostgresOperator(
    task_id='create_h3_lineage',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        ALTER TABLE h3_oceans
        ADD
            hex_04 H3INDEX;
        ALTER TABLE h3_oceans
        ADD
            hex_03 H3INDEX;
        ALTER TABLE h3_oceans
        ADD
            hex_02 H3INDEX;
        ALTER TABLE h3_oceans
        ADD
            hex_01 H3INDEX;
        ALTER TABLE h3_oceans
        ADD
            hex_00 H3INDEX;
    """,
)


# Task: Create the h3_children table
fill_h3_lineage = PostgresOperator(
    task_id='fill_h3_lineage',
    postgres_conn_id='oceexp-db',  # Define your connection ID
    sql="""
        UPDATE h3_oceans
        SET 
            hex_04 = h3_cell_to_parent(hex_05, 4);
        UPDATE h3_oceans
        SET 
            hex_03 = h3_cell_to_parent(hex_05, 3);
        UPDATE h3_oceans
        SET
            hex_02 = h3_cell_to_parent(hex_05, 2);
        UPDATE h3_oceans
        SET
            hex_01 = h3_cell_to_parent(hex_05, 1);
        UPDATE h3_oceans
        SET
            hex_00 = h3_cell_to_parent(hex_05, 0);
    """,
)


# Set task dependencies
download_task >> process_task >> load_h3_task >> create_h3_primary >> create_h3_lineage >> fill_h3_lineage
