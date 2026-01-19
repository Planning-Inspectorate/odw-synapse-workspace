from pyspark.sql import SparkSession
from delta import *


DATABASE_NAMES = [
    "odw_standardised_db",
    "odw_harmonised_db",
    "odw_curated_db"
]


def pytest_runtest_setup(item):
    # Initialise the first spark session with the below settings
    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    ).getOrCreate()
    for database in DATABASE_NAMES:
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
