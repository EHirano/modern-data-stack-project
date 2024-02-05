from datetime import datetime, timedelta
import boto3

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator, GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup

from function.group_socios import 

default_args = {
    "owner": "Eduardo Lovera Hirano",
    # 'on_failure_callback': notificate
}

BUCKET = 's3://dev-benice-companies-dataset/companies_raw/'

@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(crawler_name: str, job_name: str, db_name: str):
    client: BaseClient = boto3.client("glue")

    client.delete_crawler(Name=crawler_name)
    client.delete_job(Name=job_name)
    client.delete_database(Name=db_name)

@task(task_id="query_read_results_from_s3")
def read_companies_from_s3(query_execution_id):
    dyf = glue_context.create_dynamic_frame_from_options(
            connection_type = "s3",
            connection_options = {
                    "paths": [
                            f"s3://{bucket}/{partition_string}"
                        ]
                },
            format = "parquet"
        )
    
    return dyf


@task(task_id="write_dataframe_to_s3")
def write_dataframe_to_s3(df, glue_context, path):
    try:
        dyf = DynamicFrame.fromDF(df, glue_context, "dyf")

        gc = GlueContext(SparkContext.getOrCreate())
        gc.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="s3",
                connection_options={
                        "path": path,
                        "partitionKeys": []
                    },
                format="parquet"
            )
        
        return True
    except Exception as e:
        print("ERROR_WRITING_GROUPED_SOCIOS_DF", {}, f"Error: {e}")
    

with DAG(
    dag_id="CompaniesGroupSocios",
    description="version: 1.0.2",
    default_args=default_args,
    start_date=datetime(2024, 2, 2),
    catchup=False,
    tags=["Companies", "Glue", "Socios", "Dev"],
    # schedule_interval="30 * * * *",
    max_active_runs=1,
) as dag:
    env_id
    start = DummyOperator(task_id="start")
    glue_crawler_config = {
        "Name": glue_crawler_name,
        "Role": role_arn,
        "DatabaseName": glue_db_name,
        "Targets": {
            "S3Targets": [
                {"Path": f"{bucket_name}/input"}
            ]
        }
    }

    
    crawl_s3 = GlueCrawlerOperator(
        task_id="crawl_s3_socios",
        config=glue_crawler_config
    )

    submit_glue_job = GlueJobOperator(
        task_id="submit_glue_job",
        job_name=group_socios,
        script_location=f's3://{BUCKET}/group_socios.py,
    )



    end = DummyOperator(task_id="end")

    (
        start
        >> load_glue_partitions
        >> update_brokers_tracker_staging
        >> merge_dimensions
        >> merge_numeric_dimensions
        >> insert_brokers_payload_fact
        >> set_staging_as_processed1
        >> insert_brokers_fact
        >> set_staging_as_processed2
        >> end
    )
    # start >> update_brokers_tracker_staging >> merge_dimensions >> merge_numeric_dimensions >> insert_brokers_fact >> set_staging_as_processed >> end

    # start >> drop_dependants >> extract_brokers >> extract_login >> extract_capabilities >> extract_iugo >> transform >> populate_temporada >> log_last_pipeline_success >> end
    # start >> merge_dimensions >> merge_numeric_dimensions >> insert_brokers_fact >> end
# [END madman]
