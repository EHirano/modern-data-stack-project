import time
from datetime import datetime, timedelta

import boto3

client = boto3.client("athena", region_name="us-east-1")


PLATFORMS = ["agro-hub", "avm", "brokers", "location"]


def create_companies_partitions(data_sources: str, date: datetime, partitions: list):
    for data_source in data_sources:
        partition = f"""PARTITION (data_source='{data_source}, year='{date.year}', month='{date.month}') LOCATION 's3://dev-benice-companies-dataset/processed/records/data_source={data_source}/year={date.year}/month={date.month}/'"""
        partitions.append(partition)

    return partitions


def create_tracking_all_day_partitions(platform: str, date: datetime, partitions: list):
    for hour in range(0, 24):
        partition = f"""PARTITION (platform='{platform}', year='{date.year}', month='{date.month}', day='{date.day}', hour='{hour}') LOCATION 's3://prd-benice-tracking/records/platform={platform}/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}'"""
        partitions.append(partition)

    return partitions


def create_partitions(date: datetime, partitions: list):
    for platform in PLATFORMS:
        partition = f"""PARTITION (platform='{platform}', year='{date.year}', month='{date.month}', day='{date.day}', hour='{date.hour}') LOCATION 's3://prd-benice-tracking/records/platform={platform}/year={date.year}/month={date.month}/day={date.day}/hour={date.hour}'"""
        partitions.append(partition)

    return partitions


def create_add_partition_query(partitions: list):
    return f"""ALTER TABLE prd_benice.tracking_base ADD IF NOT EXISTS {" ".join(partitions)};"""


def start_query(query: str):
    res = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            "OutputLocation": "s3://aws-athena-query-results-953075382512-us-east-1/"
        },
    )
    print(res)


def create_last_three_months_partitions():
    today = datetime.today().replace(minute=0, second=0, microsecond=0)
    start_at = today - timedelta(days=120)

    partitions = []
    while True:
        start_at += timedelta(hours=1)

        if start_at > today:
            break

        partitions = create_partitions(start_at, partitions)
        if len(partitions) >= (5 * 24 * 4):
            query = create_add_partition_query(partitions)

            start_query(query)
            time.sleep(30)
            partitions = []

    if len(partitions) > 0:
        query = create_add_partition_query(partitions)
        start_query(query)
