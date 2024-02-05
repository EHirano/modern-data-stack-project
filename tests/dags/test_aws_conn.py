from datetime import datetime

from airflow.decorators import dag
from airflow.models.connection import Connection  # Deprecated in Airflow 2

if __name__ == "__main__":
    dag.test(execution_date=datetime(), conn_file_path=conn_path)
