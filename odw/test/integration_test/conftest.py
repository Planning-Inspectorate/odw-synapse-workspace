from pyspark.sql import SparkSession
from delta import *
import os
import json


DATABASE_NAMES = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]


def create_empty_orchestration_file():
    orchestration_path = os.path.join("spark-warehouse", "odw-config", "orchestration")
    os.makedirs(orchestration_path, exist_ok=True)
    with open(os.path.join("spark-warehouse", "odw-config", "orchestration", "orchestration.json"), "w") as f:
        content = {"definitions": []}
        json.dump(content, f, indent=4)


def pytest_runtest_setup(item):
    # Initialise the first spark session with the below settings
    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    ).getOrCreate()
    for database in DATABASE_NAMES:
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    create_empty_orchestration_file()
