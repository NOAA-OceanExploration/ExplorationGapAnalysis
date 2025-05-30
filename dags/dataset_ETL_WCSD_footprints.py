import requests
import json
import datetime
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import h3
import json
import time
import logging
import shapely
from shapely.geometry import shape  # Added import

def fetch_WCSD_footprints():
    # Define the base URL for the Feature Layer
    base_url = "https://services2.arcgis.com/C8EMgrsFcRFL6LrL/ArcGIS/rest/services/Water_Column_Sonar_Datasets/FeatureServer/1/query"

    # Define parameters for the query
    params = {
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",  # GeoJSON output
        "geometryType": "esriGeometryPolyline",
        "returnGeometry": "true",
        "resultRecordCount": 2000,  # Max per request; paginate if more records needed
    }

    # Make the request
    response = requests.get(base_url, params=params)

    # Check response
    if response.status_code == 200:
        geojson = response.json()

        # Save to file
        with open("/mnt/bucket/polylines.geojson", "w") as f:
            json.dump(geojson, f, indent=2)

        print("Downloaded and saved polylines to polylines.geojson")
    else:
        print(f"Request failed with status code {response.status_code}")


def load_geojson_to_postgres():
    # Load GeoJSON
    with open("/mnt/bucket/polylines.geojson") as f:
        data = json.load(f)

    # Get Postgres connection
    pg_hook = PostgresHook(postgres_conn_id="oceexp-db")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Optionally drop and recreate the table
    cursor.execute("""
        DROP TABLE IF EXISTS wcsd_lines;
                   
        CREATE TABLE wcsd_lines (
            wcsdid SERIAL PRIMARY KEY,
            objectid INTEGER,
            cruise_name TEXT,
            dataset_name TEXT,
            platform_name TEXT,
            departure_port TEXT,
            arrival_port TEXT,
            instrument_name TEXT,
            frequency TEXT,
            citation_link TEXT,
            cloud_path TEXT,
            shape_length DOUBLE PRECISION,
            start_date TEXT,
            end_date TEXT,
            geom GEOMETRY(MULTILINESTRING, 3857)
        );
    """)

    conn.commit()

    insert_sql = """
        INSERT INTO wcsd_lines (
            objectid, cruise_name, dataset_name, platform_name,
            departure_port, arrival_port, instrument_name, frequency,
            citation_link, cloud_path, shape_length, start_date,
            end_date, geom
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        ST_GeomFromText(%s, 3857));
    """

    for feature in data["features"]:
        props = feature["properties"]
        geom = shape(feature["geometry"])
        wkt = geom.wkt

        values = (
            props.get("OBJECTID"),
            props.get("CRUISE_NAME"),
            props.get("DATASET_NAME"),
            props.get("PLATFORM_NAME"),
            props.get("DEPARTURE_PORT"),
            props.get("ARRIVAL_PORT"),
            props.get("INSTRUMENT_NAME"),
            props.get("FREQUENCY"),
            props.get("CITATION_LINK"),
            props.get("CLOUD_PATH"),
            props.get("Shape__Length"),
            props.get("START_DATE"),
            props.get("END_DATE"),
            wkt
        )

        cursor.execute(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("GeoJSON features successfully loaded using raw SQL and PostgresHook.")


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
    'dataset_ETL_WCSD_footprints',
    default_args=default_args,
    description='Fetch WCSD spatial coverage from REST services, save to PostgreSQL, assign hexes',
    schedule_interval=None,
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

fetch_WCSD_footprints = PythonOperator(
    task_id='fetch_WCSD_footprints',
    python_callable=fetch_WCSD_footprints,
    provide_context=True,
    dag=dag
)

load_geojson_to_postgres = PythonOperator(
    task_id='load_geojson_to_postgres',
    python_callable=load_geojson_to_postgres,
    provide_context=True,
    dag=dag
)

# Task: propagate scores upwards
points_to_hex_table= PostgresOperator(
    task_id='points_to_hex_table',
    postgres_conn_id='oceexp-db',
    sql="""
        DROP TABLE IF EXISTS wcsd_footprints;

        CREATE TABLE wcsd_footprints AS
        WITH line_length AS (
            SELECT wcsdid, 
                ST_Length(geom) AS line_length
            FROM wcsd_lines
        ),
        points AS (
            SELECT wcsd_lines.wcsdid,
                ST_LineInterpolatePoint((ST_Dump(wcsd_lines.geom)).geom, loc / line_length.line_length) AS point
            FROM wcsd_lines
            JOIN line_length ON wcsd_lines.wcsdid = line_length.wcsdid
            CROSS JOIN generate_series(0, (line_length.line_length / 500)::int) AS loc
        )
        SELECT wcsdid, 
            point,
            h3_lat_lng_to_cell(point, 5) AS hex_05
        FROM points;

        ALTER TABLE wcsd_footprints 
            ADD CONSTRAINT fk_wcsdid_lines
            FOREIGN KEY (wcsdid) REFERENCES wcsd_lines (wcsdid);
        """,
    dag=dag
)

fetch_WCSD_footprints >> load_geojson_to_postgres >> points_to_hex_table