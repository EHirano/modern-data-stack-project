from datetime import datetime

import boto3
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)

# from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
# from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "Eduardo Lovera Hirano",
    # 'on_failure_callback': notificate
}

ENV = "dev"
BUCKET = "dev-benice-companies-dataset"
S3_SOCIOS_RAW_KEY = "companies_raw"
S3_KEY_GLUE_SCRIPT = "companies_scripts/group_socios.py"


@task(task_id="extract_files_to_download")
def extract_files_to_download():
    return [
        {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios1.zip"},
        # {"sftp_path": "https://dadosabertos.rfb.gov.br/CNPJ/Socios2.zip"},
    ]


@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(crawler_name: str, job_name: str):
    client = boto3.client("glue")

    client.delete_crawler(Name=crawler_name)
    client.delete_job(Name=job_name)


@dag(
    dag_id="Companies",
    start_date=datetime(2024, 3, 2),
    max_active_runs=1,
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    tags=["companies", "dev", "ingestion", "s3", "data lake"],
)
def companies():

    start = EmptyOperator(task_id="start")

    glue_job_name = f"{ENV}_group_socios"
    glue_crawler_name = f"{ENV}_group_socios_crawler"

    glue_crawler_config = {"DatabaseName": "dev_benice", "Targets": {"S3Targets": [{"Path": f"s3://{BUCKET}/{S3_SOCIOS_RAW_KEY}"}]}}

    with open("dags/companies/group_socios.py", "r") as python_file:
        script_content = python_file.read()

    # ingest_data_from_sftp_to_s3 = SFTPToS3Operator.partial(s3_bucket=BUCKET, s3_key=f"/{S3_SOCIOS_RAW_KEY}", use_temp_file=True).expand_kwargs(
    #     extract_files_to_download()
    # )

    crawl_s3_socios = GlueCrawlerOperator(task_id="crawl_s3_socios", config=glue_crawler_config, wait_for_completion=True)

    upload_script = S3CreateObjectOperator(task_id="upload_script", s3_bucket=BUCKET, s3_key=f"/{S3_KEY_GLUE_SCRIPT}", data=script_content)

    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        job_name=glue_job_name,
        script_location=f"s3://{BUCKET}/{S3_KEY_GLUE_SCRIPT}",
        s3_bucket=f"{BUCKET}/{S3_SOCIOS_RAW_KEY}",
        create_job_kwargs={"GlueVersion": 4.0, "NumberOfWorkers": 2, "WorkerType": "G.1X"},
        wait_for_completion=True,
    )

    # wait_glue_job = GlueJobSensor(task_id="wait_glue_job", job_name=glue_job_name, run_id=submit_glue_job.output, verbose=True)

    # wait_glue_job.poke_interval = 5

    delete_raw_socios_bucket = S3DeleteBucketOperator(
        task_id="delete_raw_socios_bucket", trigger_rule=TriggerRule.ALL_DONE, bucket_name=f"{BUCKET}/{S3_SOCIOS_RAW_KEY}", force_delete=True
    )

    end = EmptyOperator(task_id="end")

    (
        start
        # >> ingest_data_from_sftp_to_s3
        >> crawl_s3_socios
        >> upload_script
        >> submit_glue_job
        # >> wait_glue_job
        >> glue_cleanup(glue_crawler_name, glue_job_name)
        >> delete_raw_socios_bucket
        >> end
    )


companies()
