import boto3
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator

# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import (
    GlueCrawlerOperator,
    GlueJobOperator,
)
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import SystemTestContextBuilder

ROLE_ARN_KEY = ""

sys_test_context_task = SystemTestContextBuilder().add_variable(ROLE_ARN_KEY).build()

@task(trigger_rule=TriggerRule.ALL_DONE)
def glue_cleanup(crawler_name: str, job_name: str, db_name: str):
    client = boto3.client("glue")

    client.delete_crawler(Name=crawler_name)
    client.delete_job(Name=job_name)
    client.delete_database(Name=db_name)


@task(task_id="query_read_results_from_s3")
def read_companies_from_s3(query_execution_id):
    dyf = glue_context.create_dynamic_frame_from_options(
        connection_type="s3",
        connection_options={"paths": [f"s3://{bucket}/{partition_string}"]},
        format="parquet",
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
            connection_options={"path": path, "partitionKeys": []},
            format="parquet",
        )

        return True
    except Exception as e:
        print("ERROR_WRITING_GROUPED_SOCIOS_DF", {}, f"Error: {e}")


glue_crawler_config = {
    "Name": glue_crawler_name,
    "Role": role_arn,
    "DatabaseName": glue_db_name,
    "Targets": {"S3Targets": [{"Path": f"{bucket_name}/input"}]},
}

crawl_s3 = GlueCrawlerOperator(task_id="crawl_s3_socios", config=glue_crawler_config)


submit_glue_job = GlueJobOperator(
    task_id="submit_glue_job",
    job_name=group_socios,
    script_location=f"s3://{BUCKET}/group_socios.py",
)


@task_group
def group_socios():
    read_companies_from_s3(glue_cleanup())
