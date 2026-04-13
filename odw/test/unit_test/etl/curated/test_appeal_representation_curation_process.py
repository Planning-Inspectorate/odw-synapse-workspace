from odw.core.etl.transformation.curated.appeal_representation_curation_process import AppealRepresentationCurationProcess
from odw.core.util.util import Util
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal
from pyspark.sql import SparkSession
import pyspark.sql.types as T
from datetime import datetime
import pytest
import mock


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


class TestAppealRepresentationCurationProcess(SparkTestCase):
    def test__appeal_representation__load_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        spark_sql_side_effects = [
            spark.createDataFrame(
                data=(
                    (
                        1,
                        2,
                        "a",
                        "published",
                        "some description",
                        False,
                        "undefined",
                        "undefined",
                        [],
                        [],
                        "lpa",
                        "undefined",
                        "final_comment",
                        datetime(2025, 1, 22, 13, 48, 35, 847).isoformat(),
                        ["x"],
                    ),
                    (
                        2,
                        3,
                        "b",
                        "published",
                        "some description",
                        False,
                        "undefined",
                        "undefined",
                        [],
                        [],
                        "lpa",
                        "undefined",
                        "final_comment",
                        datetime(2025, 1, 23, 13, 48, 35, 847).isoformat(),
                        ["y"],
                    ),
                ),
                schema=T.StructType(
                    [
                        T.StructField("representationId", T.StringType(), True),
                        T.StructField("caseId", T.LongType(), True),
                        T.StructField("caseReference", T.StringType(), True),
                        T.StructField("representationStatus", T.StringType(), True),
                        T.StructField("originalRepresentation", T.StringType(), True),
                        T.StructField("redacted", T.BooleanType(), True),
                        T.StructField("redactedRepresentation", T.StringType(), True),
                        T.StructField("redactedBy", T.StringType(), True),
                        T.StructField("invalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                        T.StructField("otherInvalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                        T.StructField("source", T.StringType(), True),
                        T.StructField("serviceUserId", T.StringType(), True),
                        T.StructField("representationType", T.StringType(), True),
                        T.StructField("dateReceived", T.StringType(), True),
                        T.StructField("documentIds", T.ArrayType(T.StringType(), True), True),
                    ]
                ),
            ),
            spark.createDataFrame(
                data=(("Location", "abfss://odw-curated@pinsstodwdevuks9h80mb.dfs.core.windows.net/appeal_representation", None),),
                schema=T.StructType(
                    [
                        T.StructField("col_name", T.StringType(), False),
                        T.StructField("data_type", T.StringType(), False),
                        T.StructField("comment", T.StringType(), True),
                    ]
                ),
            ),
        ]
        expected_output = {"harmonised_data": spark_sql_side_effects[0], "curated_data_description": spark_sql_side_effects[1]}
        with mock.patch.object(SparkSession, "sql", side_effect=spark_sql_side_effects):
            with mock.patch.object(AppealRepresentationCurationProcess, "__init__", return_value=None):
                inst = AppealRepresentationCurationProcess()
                actual_output = inst.load_data()
                expected_keys = set(expected_output.keys())
                actual_keys = set(actual_output.keys())
                assert expected_keys == actual_keys, (
                    f"Expected a dictionary with keys {expected_keys} to be returned by load_data(), but received the keys {actual_keys} instead"
                )
                assert_dataframes_equal(expected_output["harmonised_data"], actual_output["harmonised_data"])
                assert_dataframes_equal(expected_output["curated_data_description"], actual_output["curated_data_description"])

    def test__appeal_representation__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        mock_data = {
            "harmonised_data": spark.createDataFrame(
                data=(
                    (
                        1,
                        2,
                        "a",
                        "published",
                        "some description",
                        False,
                        "undefined",
                        "undefined",
                        [],
                        [],
                        "lpa",
                        "undefined",
                        "final_comment",
                        datetime(2025, 1, 22, 13, 48, 35, 847).isoformat(),
                        ["x"],
                    ),
                    (
                        2,
                        3,
                        "b",
                        "published",
                        "some description",
                        False,
                        "undefined",
                        "undefined",
                        [],
                        [],
                        "lpa",
                        "undefined",
                        "final_comment",
                        datetime(2025, 1, 23, 13, 48, 35, 847).isoformat(),
                        ["y"],
                    ),
                ),
                schema=T.StructType(
                    [
                        T.StructField("representationId", T.StringType(), True),
                        T.StructField("caseId", T.LongType(), True),
                        T.StructField("caseReference", T.StringType(), True),
                        T.StructField("representationStatus", T.StringType(), True),
                        T.StructField("originalRepresentation", T.StringType(), True),
                        T.StructField("redacted", T.BooleanType(), True),
                        T.StructField("redactedRepresentation", T.StringType(), True),
                        T.StructField("redactedBy", T.StringType(), True),
                        T.StructField("invalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                        T.StructField("otherInvalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                        T.StructField("source", T.StringType(), True),
                        T.StructField("serviceUserId", T.StringType(), True),
                        T.StructField("representationType", T.StringType(), True),
                        T.StructField("dateReceived", T.StringType(), True),
                        T.StructField("documentIds", T.ArrayType(T.StringType(), True), True),
                    ]
                ),
            ),
            "curated_data_description": spark.createDataFrame(
                data=(("Location", "abfss://odw-curated@pinsstodwdevuks9h80mb.dfs.core.windows.net/appeal_representation", None),),
                schema=T.StructType(
                    [
                        T.StructField("col_name", T.StringType(), False),
                        T.StructField("data_type", T.StringType(), False),
                        T.StructField("comment", T.StringType(), True),
                    ]
                ),
            ),
        }
        with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
            expected_data_to_write = {
                "odw_curated_db.appeal_representation": {
                    "data": mock_data["harmonised_data"],
                    "storage_kind": "ADLSG2-Table",
                    "database_name": "odw_curated_db",
                    "table_name": "appeal_representation",
                    "storage_endpoint": Util.get_storage_account(),
                    "container_name": "odw-curated",
                    "blob_path": "appeal_representation",
                    "file_format": "parquet",
                    "write_mode": "overwrite",
                    "write_options": {},
                }
            }
            with mock.patch.object(AppealRepresentationCurationProcess, "__init__", return_value=None):
                inst = AppealRepresentationCurationProcess()
                actual_data_to_write, _ = inst.process(mock_data)
                expected_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in expected_data_to_write.items()}
                actual_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in actual_data_to_write.items()}
                assert expected_data_to_write_without_data == actual_data_to_write_without_data
                assert (
                    expected_data_to_write["odw_curated_db.appeal_representation"]["data"]
                    == actual_data_to_write["odw_curated_db.appeal_representation"]["data"]
                )
