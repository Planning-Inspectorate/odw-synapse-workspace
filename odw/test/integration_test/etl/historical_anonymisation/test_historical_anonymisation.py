import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.historical_anonymisation.historical_anonymisation_process import (
    HistoricalAnonymisationProcess,
)
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.util import (
    get_all_files_in_directory,
    format_adls_path_to_local_path,
)
from datetime import datetime
from typing import Dict, List, Any
import csv
import json
import os
import hashlib


def create_standardised_dataframe(
    data: List[Dict[str, Any]], schema: T.StructType
) -> DataFrame:
    standardised_cols = {
        "ingested_datetime": datetime(2025, 1, 1),
        "ingested_by_process_name": "py_horizon_raw_to_std",
        "expected_from": datetime(2025, 1, 1),
        "expected_to": datetime(2025, 1, 1),
        "input_file": "some_file",
        "modified_datetime": datetime(2025, 1, 1),
        "modified_by_process_name": "py_horizon_raw_to_std",
        "entity_name": "DaRT_Inspectors",
        "file_id": "1",
    }
    standardised_cols_schema = [
        T.StructField("ingested_datetime", T.TimestampType(), True),
        T.StructField("ingested_by_process_name", T.StringType(), True),
        T.StructField("expected_from", T.TimestampType(), True),
        T.StructField("expected_to", T.TimestampType(), True),
        T.StructField("input_file", T.StringType(), True),
        T.StructField("modified_datetime", T.TimestampType(), True),
        T.StructField("modified_by_process_name", T.StringType(), True),
        T.StructField("entity_name", T.StringType(), True),
        T.StructField("file_id", T.StringType(), True),
    ]
    enriched_data = [x | standardised_cols for x in data]
    spark = PytestSparkSessionUtil().get_spark_session()
    return spark.createDataFrame(
        enriched_data, schema=T.StructType(schema.fields + standardised_cols_schema)
    )


class TestHistoricalAnonymisationProcess(ETLTestCase):
    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        with (
            mock.patch.object(LoggingUtil, "__new__"),
            mock.patch.object(LoggingUtil, "log_info", return_value=None),
            mock.patch.object(LoggingUtil, "log_error", return_value=None),
            mock.patch.object(
                Util,
                "get_storage_account",
                return_value="test-storage.dfs.core.windows.net",
            ),
            mock.patch.object(
                SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path
            ),
            mock.patch.object(
                HistoricalAnonymisationProcess,
                "get_all_files_in_directory",
                get_all_files_in_directory,
            ),
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
        ):
            yield

    def write_csv(self, data: Any, path: List[str]):
        directories = path[:-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w+", newline="") as file:
            if data:
                keys = keys = data[0].keys()
                writer = csv.DictWriter(file, keys)
                writer.writeheader()
            else:
                writer = csv.writer(file)
            writer.writerows(data)

    def write_json(self, data: Any, path: List[str]):
        directories = path[:-1]
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        os.makedirs(os.path.join(warehouse_name, *directories), exist_ok=True)
        with open(os.path.join(warehouse_name, *path), "w+", newline="") as file:
            json.dump(data, file)

    def test__historical_anonymisation__run__horizon(self):
        entity_name = "t_ha_r_h"
        subfolder = "t_ha_r_h"
        spark = PytestSparkSessionUtil().get_spark_session()
        warehouse_name = PytestSparkSessionUtil().get_spark_warehouse_name()
        # Using DaRT Inspectors data
        dart_inspectors_raw = [
            {
                "horizonId": 1,
                "firstName": "Jean-Luc",
                "lastName": "Picard",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "James T Kirk",
                "qualifications": "BSc",
                "email": "jlp@starfleet.com",
            },
            {
                "horizonId": 2,
                "firstName": "William",
                "lastName": "Riker",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "Jean-Luc Picard",
                "qualifications": "BA",
                "email": "wtr@starfleet.com",
            },
            {
                "horizonId": 3,
                "firstName": "Beverly",
                "lastName": "Crusher",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mrs",
                "salutation": "",
                "qualifications": "BSc",
                "email": "bc@starfleet.com",
            },
            {
                "horizonId": 4,
                "firstName": "Geordi",
                "lastName": "LaForge",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "Montgomery Scott",
                "qualifications": "BSc",
                "email": "gl@starfleet.com",
            },
        ]
        self.write_csv(
            dart_inspectors_raw,
            ("odw-raw", subfolder, "2025-01-01", f"{entity_name}.csv"),
        )
        # Standardised data
        horizon_pins_inspector = [
            {  # Unanonymised
                "horizonId": 1,
                "firstName": "Jean-Luc",
                "lastName": "Picard",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "James T Kirk",
                "qualifications": "BSc",
                "email": "jlp@starfleet.com",
            },
            {  # Anonymised with old format
                "horizonId": 2,
                "firstName": "W******",
                "lastName": "R****",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "J**************",
                "qualifications": "BA",
                "email": "w****************",
            },
            {  # Partially anonymised with a mixture of methods
                "horizonId": 3,
                "firstName": "REDACTED",
                "lastName": "Crusher",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mrs",
                "salutation": "",
                "qualifications": "BSc",
                "email": "b***************",
            },
            {  # Fully anonymised
                "horizonId": 4,
                "firstName": "REDACTED",
                "lastName": "REDACTED",
                "postName": "Inspector",
                "organisationName": "Inspector",
                "title": "Mr",
                "salutation": "REDACTED",
                "qualifications": "BSc",
                "email": "REDACTED",
            },
        ]
        standardised_data = create_standardised_dataframe(
            horizon_pins_inspector,
            T.StructType(
                [
                    T.StructField("horizonId", T.StringType(), True),
                    T.StructField("firstName", T.StringType(), True),
                    T.StructField("lastName", T.StringType(), True),
                    T.StructField("postName", T.StringType(), True),
                    T.StructField("organisationName", T.StringType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField("salutation", T.StringType(), True),
                    T.StructField("qualifications", T.StringType(), True),
                    T.StructField("email", T.StringType(), True),
                ]
            ),
        )
        standardised_data.write.format("parquet").mode("overwrite").save(
            f"{warehouse_name}/odw-standardised/{entity_name}"
        )
        expected_anonymised_data = create_standardised_dataframe(
            [
                {
                    "horizonId": 1,
                    "firstName": "REDACTED",
                    "lastName": "REDACTED",
                    "postName": "Inspector",
                    "organisationName": "Inspector",
                    "title": "Mr",
                    "salutation": "REDACTED",
                    "qualifications": "BSc",
                    "email": hashlib.sha256(
                        "jlp@starfleet.com".encode("utf-8")
                    ).hexdigest(),
                },
                {
                    "horizonId": 2,
                    "firstName": "REDACTED",
                    "lastName": "REDACTED",
                    "postName": "Inspector",
                    "organisationName": "Inspector",
                    "title": "Mr",
                    "salutation": "REDACTED",
                    "qualifications": "BA",
                    "email": hashlib.sha256(
                        "wtr@starfleet.com".encode("utf-8")
                    ).hexdigest(),
                },
                {
                    "horizonId": 3,
                    "firstName": "REDACTED",
                    "lastName": "REDACTED",
                    "postName": "Inspector",
                    "organisationName": "Inspector",
                    "title": "Mrs",
                    "salutation": None,
                    "qualifications": "BSc",
                    "email": hashlib.sha256(
                        "bc@starfleet.com".encode("utf-8")
                    ).hexdigest(),
                },
                {
                    "horizonId": 4,
                    "firstName": "REDACTED",
                    "lastName": "REDACTED",
                    "postName": "Inspector",
                    "organisationName": "Inspector",
                    "title": "Mr",
                    "salutation": "REDACTED",
                    "qualifications": "BSc",
                    "email": hashlib.sha256(
                        "gl@starfleet.com".encode("utf-8")
                    ).hexdigest(),
                },
            ],
            T.StructType(
                [
                    T.StructField("horizonId", T.StringType(), True),
                    T.StructField("firstName", T.StringType(), True),
                    T.StructField("lastName", T.StringType(), True),
                    T.StructField("postName", T.StringType(), True),
                    T.StructField("organisationName", T.StringType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField("salutation", T.StringType(), True),
                    T.StructField("qualifications", T.StringType(), True),
                    T.StructField("email", T.StringType(), True),
                ]
            ),
        )
        override_config = {
            entity_name: {
                "raw_blob_path": subfolder,  # For the "real" data this will be set to "Horizon" - this is set differently here to prevent conflicts
                "raw_blob_format": "csv",
                "standardised_blob_path": entity_name,
                "category": "Horizon",
                "raw_blob_read_options": {"header": "true"},
                "primary_keys": "horizonId",
                "cols_to_revert_to_raw": [
                    "firstName",
                    "lastName",
                    "salutation",
                    "email",
                ],
                "horizon_file_name": entity_name,
            }
        }
        mocked_purview_cols = [
            {"column_name": "horizonId", "classifications": []},
            {
                "column_name": "firstName",
                "classifications": ["MICROSOFT.PERSONAL.NAME"],
            },
            {"column_name": "lastName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "postName", "classifications": []},
            {"column_name": "organisationName", "classifications": []},
            {"column_name": "title", "classifications": []},
            {
                "column_name": "salutation",
                "classifications": ["MICROSOFT.PERSONAL.NAME"],
            },
            {"column_name": "qualifications", "classifications": []},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
            {"column_name": "ingested_datetime", "classifications": []},
            {"column_name": "ingested_by_process_name", "classifications": []},
            {"column_name": "expected_from", "classifications": []},
            {"column_name": "expected_to", "classifications": []},
            {"column_name": "input_file", "classifications": []},
            {"column_name": "modified_datetime", "classifications": []},
            {"column_name": "modified_by_process_name", "classifications": []},
            {"column_name": "entity_name", "classifications": []},
            {"column_name": "file_id", "classifications": []},
        ]
        with (
            mock.patch.object(
                HistoricalAnonymisationProcess, "_ENTITY_CONFIG", override_config
            ),
            mock.patch(
                "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
                return_value=mocked_purview_cols,
            ),
        ):
            etl_result = HistoricalAnonymisationProcess(
                spark,
            ).run(
                entity_name=entity_name,
                orchestration_run_id="t_ha_r_h",
                orchestration_entity_name=entity_name,
                orchestration_stage_name="historical_anonymisation",
            )
            assert_etl_result_successful(etl_result)
            actual_anonymised_data = spark.read.format("parquet").load(
                f"{warehouse_name}/odw-standardised/anonymised/{entity_name}"
            )
            assert_dataframes_equal(expected_anonymised_data, actual_anonymised_data)

    def test__historical_anonymisation__run__entraid_aiedocument(self):
        pass

    def test__historical_anonymisation__run__service_bus(self):
        pass
