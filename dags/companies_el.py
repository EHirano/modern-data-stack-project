import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator

default_args = {"owner": "Eduardo Hirano", "retries": 3, "retry_delay": 60}

BUCKET = "s3://dev-benice-companies-dataset/companies-socios"


@dag(
    dag_id="companies",
    start_date=datetime(2024, 3, 2),
    max_active_runs=1,
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=False,
    tags=["companies", "dev", "ingestion", "s3", "data lake"],
)
def ingest_data():

    start = EmptyOperator(task_id="start")

    @task
    def extract_files_to_download():
        files_to_download = [
            {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios1.zip"},
            {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios2.zip"},
        ]

        return files_to_download

    ingest_data_from_sftp_to_s3 = SFTPToS3Operator.partial().expand_kwargs(
        extract_files_to_download()
    )

    create_glue_partitions = AWSAthenaOperator()

    create_three_months_old_glue_partition = AWSAthenaOperator()

    finish = EmptyOperator(task_id="finish")
