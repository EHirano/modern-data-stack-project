from datetime import datetime

import boto3
from airflow.decorators import task

client = boto3.client("athena", region_name="us-east-1")

ENV = "dev"


def create_partitions_queries(action: str, date: datetime):
    data_sources = [
        {"socios": "companies_socios"},
        {"empresas": "companies_empresas"},
        {"estabelecimentos": "companies_estabelecimentos"},
        {"simples": "companies_simples"},
    ]

    partitions = []
    for data_source, table_name in data_sources.items():
        partition = f"""PARTITION (data_source='{data_source}, year='{date.year}', month='{date.month}') \
            LOCATION 's3://dev-benice-companies-dataset/processed/records/data_source={data_source}/year={date.year}/month={date.month}/'"""

        if action == "add":
            partitions.append(
                f"""ALTER TABLE {ENV}_benice.{table_name} ADD IF NOT EXISTS {partition};"""
            )

        if action == "drop":
            partitions.append(f"""ALTER TABLE {ENV}_benice.{table_name} DROP {partition};""")

    return partitions


def start_partitions_queries_executions(queries):
    query_execution_ids = []
    for query in queries:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": f"{ENV}_benice"},
            ResultConfiguration={f"OutputLocation": "s3://{ENV}-benice-companies-dataset/queries"},
        )

        query_execution_ids.append(response["QueryExecutionId"])

    return query_execution_ids


def check_athena_query_execution_status(query_execution_id: str, query_type: str, retry_count=100):
    for _ in range(retry_count):
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        state = response["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            query_runtime = client.get_query_runtime_statistics(QueryExecutionId=query_execution_id)
            print(f"{query_type}_QUERY_SUCCEEDED", query_runtime["QueryRuntimeStatistics"])
            return True

        if state in ("FAILED", "CANCELLED"):
            if (
                response["QueryExecution"]["Status"]["AthenaError"]["ErrorMessage"]
                == "Partition already exists."
            ):
                print("PARTITION_ALREADY_EXISTS")

                return True

            print(f"{query_type}_QUERY_EXECUTION_FAILED", data=response["QueryExecution"])
            return False

        print(0.2)  # each 0.2 s for 20s

    print(f"{query_type}_QUERY_EXECUTION_TIMEOUT", f"query_execution_id: {query_execution_id}")
    return False


@task
def add_partitions():
    datetime_now = datetime.now()
    partitions_queries = create_partitions_queries("add", datetime_now)
    query_execution_ids = start_partitions_queries_executions(partitions_queries)
    for query_id in query_execution_ids:
        check_athena_query_execution_status(query_id, query_type="ADD_PARTITIONS")

@task
def delete_partitions():
    datetime_now = datetime.now()
    partitions_queries = create_partitions_queries("drop", datetime_now)
    query_execution_ids = start_partitions_queries_executions(partitions_queries)
    for query_id in query_execution_ids:
        check_athena_query_execution_status(query_id, query_type="ADD_PARTITIONS")