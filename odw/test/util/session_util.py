from odw.test.util.singleton import Singleton
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pyspark.sql.types as T
from uuid import uuid4
import os
import json
import shutil


class PytestSparkSessionUtil(metaclass=Singleton):
    """
    Class to manage the testing session itself. This initiallises a single spark context per worker thread,
    alongside a separate DB/filesystem per thread. This class is a singleton, so only one instance can ever
    be created

    # Usage 
    - `PytestSessionUtil().get_spark_session()`
    - `PytestSessionUtil().get_thread_id()`
    - `PytestSessionUtil().get_spark_warehouse_name()`
    """
    DATABASE_NAMES = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]
    def __init__(self, *args, **kwargs):
        self._THREAD_ID = str(uuid4())[:8]
        self._SPARK_SESSION = configure_spark_with_delta_pip(
            SparkSession.builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.warehouse.dir", self.get_spark_warehouse_name())
            .config("spark.ui.enabled", False)
        ).getOrCreate()
        self._initialise_file_system(self._SPARK_SESSION)

    def get_thread_id(self):
        return self._THREAD_ID

    def get_spark_warehouse_name(self):
        return f"spark-warehouse-{self.get_thread_id()}"

    def get_spark_session(self):
        return self._SPARK_SESSION
    
    def _initialise_file_system(self, spark_session: SparkSession):
        for database in self.DATABASE_NAMES:
            spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
        self._create_empty_orchestration_file()
        self._create_main_source_system_fact_table(spark_session)

    def teardown_all_file_systems(self):
        file_systems = [entry for entry in os.listdir(".") if entry.startswith("spark-warehouse") and os.path.isdir(entry)]
        for file_system in file_systems:
            shutil.rmtree(file_system, ignore_errors=True)

    def _create_empty_orchestration_file(self):
        orchestration_path = os.path.join(self.get_spark_warehouse_name(), "odw-config", "orchestration")
        os.makedirs(orchestration_path, exist_ok=True)
        with open(os.path.join(self.get_spark_warehouse_name(), "odw-config", "orchestration", "orchestration.json"), "w") as f:
            content = {"definitions": []}
            json.dump(content, f, indent=4)

    def _create_main_source_system_fact_table(self, spark: SparkSession):
        shutil.rmtree(os.path.join(self.get_spark_warehouse_name(), "odw_harmonised_db.db", "main_sourcesystem_fact"), ignore_errors=True)
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
