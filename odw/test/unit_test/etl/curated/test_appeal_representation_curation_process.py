from odw.core.etl.transformation.curated.appeal_representation_curation_process import AppealRepresentationCurationProcess
from odw.core.util.util import Util
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal
import pyspark.sql.types as T
from datetime import datetime
import pytest
import mock
import pytest


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


class TestAppealRepresentationCurationProcess(SparkTestCase):
    def test__appeal_representation__load_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_data = spark.createDataFrame(
            data=(
                (
                    "1",
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
                    "1",
                    "Y",
                ),
                (
                    "2",
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
                    "1",
                    "Y",
                ),
                (  # This row will be dropped
                    "3",
                    4,
                    "c",
                    "published",
                    "another description",
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
                    "1",
                    "N",
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
                    T.StructField("extraCol", T.StringType(), True),  # Extra col to prove only specific columns are selected,
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name="tu_ar_ld__harmonised",
            database_name="odw_harmonised_db",
            container="odw-harmonised",
            blob_path="tu_ar_ld__harmonised",
            mode="overwrite",
        )
        curated_data = spark.createDataFrame(((1,),), schema=["colA"])
        self.write_existing_table(
            spark,
            curated_data,
            table_name="tu_ar_ld__curated",
            database_name="odw_curated_db",
            container="odw-curated",
            blob_path="tu_ar_ld__curated",
            mode="overwrite",
        )
        expected_fetched_harmonised_data = spark.createDataFrame(
            data=(
                (
                    "1",
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
                    "2",
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
        )
        expected_fetched_description = spark.sql("DESCRIBE DETAIL odw_curated_db.tu_ar_ld__curated")
        expected_output_keys = {"harmonised_data", "curated_data_description"}
        with mock.patch.object(AppealRepresentationCurationProcess, "__init__", return_value=None):
            inst = AppealRepresentationCurationProcess()
            actual_output = inst.load_data()
            actual_keys = set(actual_output.keys())
            assert expected_output_keys == actual_keys, (
                f"Expected a dictionary with keys {expected_output_keys} to be returned by load_data(), but received the keys {actual_keys} instead"
            )
            assert_dataframes_equal(expected_fetched_harmonised_data, actual_output["harmonised_data"])
            assert_dataframes_equal(expected_fetched_description, actual_output["curated_data_description"])

    def test__appeal_representation__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        mock_data = {
            "harmonised_data": spark.createDataFrame(
                data=(
                    (
                        "1",
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
                        "2",
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
                assert_dataframes_equal(
                    expected_data_to_write["odw_curated_db.appeal_representation"]["data"],
                    actual_data_to_write["odw_curated_db.appeal_representation"]["data"],
                )
