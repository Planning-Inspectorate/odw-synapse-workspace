import mock
import pyspark.sql.types as T
import pytest
from odw.core.etl.transformation.curated.appeal_folder_curation_process import AppealFolderCurationProcess
from odw.core.util.util import Util
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


pytestmark = pytest.mark.skip(reason="Curated logic not implemented yet")


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
        (
            "100",
            "APP/100",
            "Core Documents",
            "Dogfennau Craidd",
            "10",
            "11",
            "Decision",
            "Y",
            "ignored",
        ),
        (
            "101",
            "APP/101",
            "Case Admin",
            "Gweinyddu Achos",
            "20",
            "20",
            "Post decision",
            "N",
            "ignored",
        ),
        (
            "102",
            "APP/102",
            "Inquiry",
            "Ymchwiliad",
            "30",
            "31",
            "Pre-examination",
            None,
            "ignored",
        ),
        # Duplicate row should be removed by DISTINCT logic.
        (
            "102",
            "APP/102",
            "Inquiry",
            "Ymchwiliad",
            "30",
            "31",
            "Pre-examination",
            None,
            "ignored",
        ),
    )


def _expected_curated_rows():
    return (
        (100, "APP/100", "Core Documents", "Dogfennau Craidd", 10, "decision"),
        (101, "APP/101", "Case Admin", "Gweinyddu Achos", None, "post_decision"),
        (102, "APP/102", "Inquiry", "Ymchwiliad", 30, "pre-examination"),
    )


def _all_case_stage_harmonised_rows():
    return (
        ("201", "APP/201", "Folder 1", "Ffolder 1", "1", "9", "Pre-application", "Y", "ignored"),
        ("202", "APP/202", "Folder 2", "Ffolder 2", "2", "9", "Acceptance", "Y", "ignored"),
        ("203", "APP/203", "Folder 3", "Ffolder 3", "3", "9", "Pre-examination", "Y", "ignored"),
        ("204", "APP/204", "Folder 4", "Ffolder 4", "4", "9", "Examination", "Y", "ignored"),
        ("205", "APP/205", "Folder 5", "Ffolder 5", "5", "9", "Recommendation", "Y", "ignored"),
        ("206", "APP/206", "Folder 6", "Ffolder 6", "6", "9", "Decision", "Y", "ignored"),
        ("207", "APP/207", "Folder 7", "Ffolder 7", "7", "9", "Post decision", "Y", "ignored"),
        ("208", "APP/208", "Folder 8", "Ffolder 8", "8", "9", "Withdrawn", "Y", "ignored"),
        ("209", "APP/209", "Folder 9", "Ffolder 9", "9", "10", "Some Mixed Stage", "Y", "ignored"),
    )


class TestAppealFolderCurationProcess(SparkTestCase):
    def test__appeal_folder__load_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_data = spark.createDataFrame(data=_harmonised_rows(), schema=_harmonised_schema())

        self.write_existing_table(
            spark,
            harmonised_data,
            table_name="tu_afcp_ld__harmonised",
            database_name="odw_harmonised_db",
            container="odw-harmonised",
            blob_path="tu_afcp_ld__harmonised",
            mode="overwrite",
        )

        expected_fetched_harmonised_data = spark.createDataFrame(
            data=_expected_curated_rows(),
            schema=_curated_schema(),
        )

        expected_output_keys = {"harmonised_data"}

        with mock.patch.object(
            AppealFolderCurationProcess,
            "HARMONISED_TABLE",
            "odw_harmonised_db.tu_afcp_ld__harmonised",
        ):
            inst = AppealFolderCurationProcess(spark)
            actual_output = inst.load_data()
            actual_keys = set(actual_output.keys())

            assert expected_output_keys == actual_keys, (
                f"Expected a dictionary with keys {expected_output_keys} to be returned by load_data(), but received the keys {actual_keys} instead"
            )
            assert_dataframes_equal(expected_fetched_harmonised_data, actual_output["harmonised_data"])

    def test__appeal_folder__process(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        mock_data = {
            "harmonised_data": spark.createDataFrame(
                data=_expected_curated_rows(),
                schema=_curated_schema(),
            )
        }

        with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"):
            expected_data_to_write = {
                "odw_curated_db.appeal_folder": {
                    "data": mock_data["harmonised_data"],
                    "storage_kind": "ADLSG2-Table",
                    "database_name": "odw_curated_db",
                    "table_name": "appeal_folder",
                    "storage_endpoint": Util.get_storage_account(),
                    "container_name": "odw-curated",
                    "blob_path": "appeal_folder",
                    "file_format": "parquet",
                    "write_mode": "overwrite",
                    "write_options": {},
                }
            }

            with mock.patch.object(AppealFolderCurationProcess, "__init__", return_value=None):
                inst = AppealFolderCurationProcess()
                actual_data_to_write, _ = inst.process(mock_data)

                expected_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in expected_data_to_write.items()}
                actual_data_to_write_without_data = {k: {sk: sv for sk, sv in v.items() if sk != "data"} for k, v in actual_data_to_write.items()}

                assert expected_data_to_write_without_data == actual_data_to_write_without_data
                assert_dataframes_equal(
                    expected_data_to_write["odw_curated_db.appeal_folder"]["data"],
                    actual_data_to_write["odw_curated_db.appeal_folder"]["data"],
                )

    def test__appeal_folder__load_data__maps_all_legacy_case_stage_values_and_lowercases_unmapped(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_data = spark.createDataFrame(data=_all_case_stage_harmonised_rows(), schema=_harmonised_schema())
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name="tu_afcp_stage_map__harmonised",
            database_name="odw_harmonised_db",
            container="odw-harmonised",
            blob_path="tu_afcp_stage_map__harmonised",
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
            AppealFolderCurationProcess,
            "HARMONISED_TABLE",
            "odw_harmonised_db.tu_afcp_stage_map__harmonised",
        ):
            inst = AppealFolderCurationProcess(spark)
            actual_output = inst.load_data()
            actual_case_stage_df = actual_output["harmonised_data"].select("id", "caseStage")
            assert_dataframes_equal(expected_case_stage_df, actual_case_stage_df)
