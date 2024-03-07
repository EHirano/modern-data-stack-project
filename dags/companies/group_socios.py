from datetime import datetime

from airflow.decorators import dag, task, task_group
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from dateutil import tz
from pyspark.context import SparkContext
from pyspark.sql.functions import collect_list, first, struct

UTC = tz.gettz("UTC")

HERE = tz.gettz("America/Sao_Paulo")

BUCKET = "s3://dev-benice-companies-dataset"

date_format = "%Y-%m-%dT%H:%M:%S.%fZ"


def datetime_now(raw=False):
    if raw:
        return datetime.now().replace(tzinfo=UTC).astimezone(HERE)
    return (datetime.now().replace(tzinfo=UTC).astimezone(HERE)).strftime(date_format)


def create_date_partition_str(string_date):
    parsed_date = datetime.strptime(string_date, date_format)
    if parsed_date.month < 10:
        month = f"0{parsed_date.month}"
    else:
        month = parsed_date.month

    partition_str = f"year={parsed_date.year}/month={month}"

    return partition_str


def create_spark_context():
    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)

    return glue_context


@task(task_id="read_companies_from_s3")
def read_companies_dataset(glue_context, partition_string):
    dyf = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [f"s3://{BUCKET}/{partition_string}"]},
        format="parquet",
        groupFiles="inPartition",
        groupSize="",
    )

    return dyf


def convert_to_spark_df(dyf):
    return dyf.toDF()


def group_partners(df):
    df_grouped_socios = df.groupBy("basic_cnpj").agg(
        first("extraction_date").alias("extraction_date"),
        first("year").alias("year"),
        first("month").alias("month"),
        collect_list(struct(*[col for col in df.columns if col not in ["basic_cnpj", "extraction_date", "year", "month"]])).alias("partners"),
    )

    return df_grouped_socios


def write_dataframe_to_s3(df, glue_context, path):
    try:
        dyf = DynamicFrame.fromDF(df, glue_context, "dyf")

        gc = GlueContext(SparkContext.getOrCreate())
        gc.write_dynamic_frame.from_options(
            frame=dyf,
            connection_type="s3",
            connection_options={"path": f"s3://{BUCKET}/processed_socios/"},
            partitionKeys=["year", "month"],
            format="parquet",
            format_options={"compression": "gzip"},
        )

        return True
    except Exception as e:
        print("ERROR_WRITING_GROUPED_SOCIOS_DF", {}, f"Error: {e}")


@task_group
def group_socios_glue():
    try:
        date_partition = datetime_now()

        glue_context = create_spark_context()

        partition_string = create_date_partition_str(date_partition)

        dyf = read_companies_dataset(glue_context, partition_string)

        df = convert_to_spark_df(dyf)

        df_grouped_socios = group_partners(df)

        status = write_dataframe_to_s3(df_grouped_socios, path)

        return status
    except Exception as e:
        print("GROUP_SOCIOS_ERROR", {}, f"Error: {e}")
