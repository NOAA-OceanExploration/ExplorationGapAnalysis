from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'processing_cleanup_and_fk',
    default_args=default_args,
    description='Clean up non-ocean observations that slipped through filter, assign foreign keys',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

cleanup_nonocean_hexes = PostgresOperator(
    task_id='cleanup_nonocean_hexes',
    postgres_conn_id='oceexp-db',
    sql="""
    DELETE FROM gebco_tid_hex t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );

    DELETE FROM gbif_occurrences t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );

    DELETE FROM obis_sequences t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );

    DELETE FROM imlgs t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );

    DELETE FROM wcsd_footprints t2
    WHERE NOT EXISTS (
        SELECT 1 FROM h3_oceans t1 WHERE t1.hex_05 = t2.hex_05
    );

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
        ALTER TABLE gbif_occurrences 
            ADD CONSTRAINT fk_gbif_occurrences_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE obis_sequences 
            ADD CONSTRAINT fk_obis_sequences_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE wcsd_footprints 
            ADD CONSTRAINT fk_wcsd_footprints_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE imlgs 
            ADD CONSTRAINT fk_imlgs_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE glodap 
            ADD CONSTRAINT fk_glodap_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE gebco_tid_hex 
            ADD CONSTRAINT fk_gebco_tid_hex_hex_05 
            FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

        ALTER TABLE gebco_tid_hex 
            ADD CONSTRAINT fk_gebco_tid_hex_polygon_id 
            FOREIGN KEY (polygon_id) REFERENCES gebco_2024_polygons (polygon_id);

        ALTER TABLE gebco_2024_polygons 
            ADD CONSTRAINT fk_gebco_2024_polygons_rid 
            FOREIGN KEY (rid) REFERENCES gebco_2024 (rid);

        ALTER TABLE ega_score_05 
            ADD CONSTRAINT fk_ega_score_05_hex_05 
            FOREIGN KEY (hex_05) REFERENCES h3_oceans (hex_05);
    """,
    dag=dag
)

# Define task dependencies
cleanup_nonocean_hexes >> assign_fks
