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
    ALTER TABLE gbif_occurrences ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    ALTER TABLE obis_sequences ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    ALTER TABLE wcsd_footprints ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    ALTER TABLE imlgs ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    ALTER TABLE glodap ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);
    ALTER TABLE gebco_tid_hex ADD FOREIGN KEY (hex_05) REFERENCES ega_score_05 (hex_05);

    ALTER TABLE gebco_tid_hex ADD FOREIGN KEY (polygon_id) REFERENCES gebco_2024_polygons (polygon_id);
    ALTER TABLE gebco_2024_polygons ADD FOREIGN KEY (rid) REFERENCES gebco_2024 (rid);

    ALTER TABLE ega_score_05 ADD FOREIGN KEY (hex_05) REFERENCES h3_oceans (hex_05);
    ALTER TABLE h3_oceans ADD FOREIGN KEY (hex_04) REFERENCES ega_score_04 (hex_04);
    ALTER TABLE h3_oceans ADD FOREIGN KEY (hex_03) REFERENCES ega_score_03 (hex_03);
    """,
    dag=dag
)

# Define task dependencies
cleanup_nonocean_hexes >> assign_fks
