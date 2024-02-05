import os
from datetime import date, datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from tasks.companies.companies_glue_partitions import add_partitions, delete_partitions
from tasks.companies.group_socios import group_socios

default_args = {"owner": "Eduardo Hirano", "retries": 3, "retry_delay": 60}

BUCKET = "s3://dev-benice-companies-dataset/"


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

    def extract_files_to_download():
        return [
            {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios1.zip"},
            {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios2.zip"},
        ]

    ingest_data_from_sftp_to_s3 = SFTPToS3Operator.partial(
        s3_bucket=BUCKET, s3_key="companies_raw", use_temp_file=True
    ).expand_kwargs(extract_files_to_download())

    finish = EmptyOperator(task_id="finish")

    start << ingest_data_from_sftp_to_s3 << [add_partitions, delete_partitions] << finish
