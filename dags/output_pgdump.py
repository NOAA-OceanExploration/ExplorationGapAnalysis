from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import time
import csv
import os
import subprocess

def run_pg_dumpall(**kwargs):
    # Get connection info from Airflow
    hook = PostgresHook(postgres_conn_id='oceexp-db')
    conn = hook.get_connection('oceexp-db')

    # Extract credentials
    host = conn.host
    port = conn.port or 5432
    user = conn.login
    password = conn.password

    # Set the password env var so pg_dumpall can use it
    env = os.environ.copy()
    env["PGPASSWORD"] = password

    # Build the pg_dumpall command
    dump_cmd = [
        "pg_dumpall",
        f"-h{host}",
        f"-p{port}",
        f"-U{user}"
    ]

    # Optionally save the output somewhere
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"/mnt/s3bucket/oceexp_db_dump_{timestamp}.sql", "w") as f:
        result = subprocess.run(dump_cmd, env=env, stdout=f, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        raise Exception(f"pg_dumpall failed: {result.stderr}")
    
    return "pg_dumpall completed successfully"

# Default arguments for DAG
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
    'output_pgdump',
    default_args=default_args,
    description='DAG to dump pgsql file copy of database',
    schedule_interval=None,  # Trigger manually or modify as needed
    start_date=datetime(2025, 3, 13),
    catchup=False,
)

dump_pgsql_to_file=PythonOperator(
    task_id='dump_postgres_db',
    python_callable=run_pg_dumpall,
    provide_context=True,
    dag=dag
)

# Define task dependencies
dump_pgsql_to_file