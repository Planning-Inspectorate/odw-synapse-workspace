import mock
import pyspark.sql.types as T
import pytest
from odw.core.etl.transformation.curated.legacy_folder_data_curation_process import LegacyFolderDataCurationProcess
from odw.core.util.util import Util
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("ID", T.StringType(), True),
            T.StructField("CaseReference", T.StringType(), True),
            T.StructField("DisplayNameEnglish", T.StringType(), True),
            T.StructField("DisplayNameWelsh", T.StringType(), True),
            T.StructField("ParentFolderID", T.StringType(), True),
            T.StructField("CaseNodeId", T.StringType(), True),
            T.StructField("caseStage", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
            T.StructField("extraCol", T.StringType(), True),
        ]
    )


def _curated_schema():
    return T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("displayNameEnglish", T.StringType(), True),
            T.StructField("displayNameWelsh", T.StringType(), True),
            T.StructField("parentFolderId", T.IntegerType(), True),
            T.StructField("caseStage", T.StringType(), True),
        ]
    )


def _harmonised_rows():
    return (
        ("100", "LFD/100", "Core Docs", "Core Docs CY", "10", "11", "Decision", "Y", "ignored"),
        ("101", "LFD/101", "Admin", "Admin CY", "20", "20", "Post decision", "N", "ignored"),
        ("102", "LFD/102", "Inquiry", "Inquiry CY", "30", "31", "Pre-examination", None, "ignored"),
        ("102", "LFD/102", "Inquiry", "Inquiry CY", "30", "31", "Pre-examination", None, "ignored"),
    )


def _expected_curated_rows():
    return (
        (100, "LFD/100", "Core Docs", "Core Docs CY", 10, "decision"),
        (101, "LFD/101", "Admin", "Admin CY", None, "post_decision"),
        (102, "LFD/102", "Inquiry", "Inquiry CY", 30, "pre-examination"),
    )


def _all_case_stage_harmonised_rows():
    return (
        ("201", "LFD/201", "Folder 1", "Folder 1 CY", "1", "9", "Pre-application", "Y", "ignored"),
        ("202", "LFD/202", "Folder 2", "Folder 2 CY", "2", "9", "Acceptance", "Y", "ignored"),
        ("203", "LFD/203", "Folder 3", "Folder 3 CY", "3", "9", "Pre-examination", "Y", "ignored"),
        ("204", "LFD/204", "Folder 4", "Folder 4 CY", "4", "9", "Examination", "Y", "ignored"),
        ("205", "LFD/205", "Folder 5", "Folder 5 CY", "5", "9", "Recommendation", "Y", "ignored"),
        ("206", "LFD/206", "Folder 6", "Folder 6 CY", "6", "9", "Decision", "Y", "ignored"),
        ("207", "LFD/207", "Folder 7", "Folder 7 CY", "7", "9", "Post decision", "Y", "ignored"),
        ("208", "LFD/208", "Folder 8", "Folder 8 CY", "8", "9", "Withdrawn", "Y", "ignored"),
        ("209", "LFD/209", "Folder 9", "Folder 9 CY", "9", "10", "Some Mixed Stage", "Y", "ignored"),
    )


class TestLegacyFolderDataCurationProcess(SparkTestCase):
    def test__legacy_folder_data__load_data(self):
        test_case = "t_lfdcp_ld"
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_data = spark.createDataFrame(data=_harmonised_rows(), schema=_harmonised_schema())

        table_name = f"{test_case}_horizon_folder"
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name=table_name,
            database_name="odw_harmonised_db",
            container="odw-harmonised",
            blob_path=table_name,
            mode="overwrite",
        )

        expected_fetched_harmonised_data = spark.createDataFrame(
            data=_expected_curated_rows(),
            schema=_curated_schema(),
        )

        expected_output_keys = {"harmonised_data"}

        with mock.patch.object(
            LegacyFolderDataCurationProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{test_case}_horizon_folder",
        ):
            inst = LegacyFolderDataCurationProcess(spark)
            actual_output = inst.load_data()
            actual_keys = set(actual_output.keys())

            assert expected_output_keys == actual_keys, (
                f"Expected a dictionary with keys {expected_output_keys} to be returned by load_data(), but received the keys {actual_keys} instead"
            )
            assert_dataframes_equal(expected_fetched_harmonised_data, actual_output["harmonised_data"])

    def test__legacy_folder_data__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        mock_data = {
            "harmonised_data": spark.createDataFrame(
                data=_expected_curated_rows(),
                schema=_curated_schema(),
            )
        }

        with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
            expected_data_to_write = {
                "odw_curated_db.legacy_folder_data": {
                    "data": mock_data["harmonised_data"],
                    "storage_kind": "ADLSG2-Table",
                    "database_name": "odw_curated_db",
                    "table_name": "legacy_folder_data",
                    "storage_endpoint": Util.get_storage_account(),
                    "container_name": "odw-curated",
                    "blob_path": "legacy_folder_data",
                    "file_format": "parquet",
                    "write_mode": "overwrite",
                    "write_options": {},
                }
            }

            with mock.patch.object(LegacyFolderDataCurationProcess, "__init__", return_value=None):
                inst = LegacyFolderDataCurationProcess()
                actual_data_to_write, _ = inst.process(mock_data)

                expected_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in expected_data_to_write.items()}
                actual_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in actual_data_to_write.items()}

                assert expected_data_to_write_without_data == actual_data_to_write_without_data
                assert_dataframes_equal(
                    expected_data_to_write["odw_curated_db.legacy_folder_data"]["data"],
                    actual_data_to_write["odw_curated_db.legacy_folder_data"]["data"],
                )

    def test__legacy_folder_data__load_data__maps_all_legacy_case_stage_values_and_lowercases_unmapped(self):
        test_case = "t_lfdcp_ld_mlcsvlu"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_data = spark.createDataFrame(data=_all_case_stage_harmonised_rows(), schema=_harmonised_schema())
        table_name = f"{test_case}_horizon_folder"
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name=table_name,
            database_name="odw_harmonised_db",
            container="odw-harmonised",
            blob_path=table_name,
            mode="overwrite",
        )

        expected_case_stage_df = spark.createDataFrame(
            data=(
                (201, "pre-application"),
                (202, "acceptance"),
                (203, "pre-examination"),
                (204, "examination"),
                (205, "recommendation"),
                (206, "decision"),
                (207, "post_decision"),
                (208, "withdrawn"),
                (209, "some mixed stage"),
            ),
            schema=T.StructType(
                [
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("caseStage", T.StringType(), True),
                ]
            ),
        )

        with mock.patch.object(
            LegacyFolderDataCurationProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{test_case}_horizon_folder",
        ):
            inst = LegacyFolderDataCurationProcess(spark)
            actual_output = inst.load_data()
            actual_case_stage_df = actual_output["harmonised_data"].select("id", "caseStage")
            assert_dataframes_equal(expected_case_stage_df, actual_case_stage_df)
