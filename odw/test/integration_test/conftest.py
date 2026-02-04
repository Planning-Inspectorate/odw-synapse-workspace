from odw.test.util.conftest_util import configure_session, session_setup, session_teardown  # noqa: F401
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from delta import configure_spark_with_delta_pip
import os
import json
import shutil


DATABASE_NAMES = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]


def create_empty_orchestration_file():
    orchestration_path = os.path.join("spark-warehouse", "odw-config", "orchestration")
    os.makedirs(orchestration_path, exist_ok=True)
    with open(os.path.join("spark-warehouse", "odw-config", "orchestration", "orchestration.json"), "w") as f:
        content = {"definitions": []}
        json.dump(content, f, indent=4)


def create_main_source_system_fact_table(spark: SparkSession):
    shutil.rmtree(os.path.join("spark-warehouse", "odw_harmonised_db.db", "main_sourcesystem_fact"), ignore_errors=True)
    data = spark.createDataFrame(
        (
            ("1", "Casework", "", None, "", "Y"),
        ),
        T.StructType(
            [
                T.StructField("SourceSystemId", T.StringType()),
                T.StructField("Description", T.StringType()),
                T.StructField("IngestionDate", T.StringType()),
                T.StructField("ValidTo", T.StringType()),
                T.StructField("RowID", T.StringType()),
                T.StructField("IsActive", T.StringType())
            ]
        )
    )
    database_name = "odw_harmonised_db"
    table_name = "main_sourcesystem_fact"
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
    write_mode = "overwrite"
    table_path = f"{database_name}.{table_name}"
    writer = data.write.format("parquet").mode(write_mode)
    writer.saveAsTable(table_path)


def pytest_sessionstart(session):
    # Initialise the first spark session with the below settings
    spark_session = configure_spark_with_delta_pip(
        SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
    ).getOrCreate()
    for database in DATABASE_NAMES:
        spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    create_empty_orchestration_file()
    create_main_source_system_fact_table(spark_session)
    configure_session()
