from datetime import datetime

import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T
import pytest
from odw.core.etl.transformation.curated.appeal_folder_curation_process import AppealFolderCurationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


class TestAppealFolderCurationProcess(ETLTestCase):
    ACTIVE_HARMONISED_DATA = [
        (
            "100",
            "APP/100",
            "Core Documents",
            "Dogfennau Craidd",
            "10",
            "11",
            "Decision",
            datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
            None,
            "row-100",
            "Y",
            "message-100",
        ),
        (
            "101",
            "APP/101",
            "Case Admin",
            "Gweinyddu Achos",
            "20",
            "20",
            "Post decision",
            datetime(2025, 1, 2, 0, 0, 0, 0).isoformat(),
            None,
            "row-101",
            "N",
            "message-101",
        ),
        (
            "102",
            "APP/102",
            "Inquiry",
            "Ymchwiliad",
            "30",
            "31",
            "Pre-examination",
            datetime(2025, 1, 3, 0, 0, 0, 0).isoformat(),
            None,
            "row-102",
            None,
            "message-102",
        ),
    ]

    def generate_harmonised_table(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        return spark.createDataFrame(
            data=self.ACTIVE_HARMONISED_DATA
            + [
                (
                    "102",
                    "APP/102",
                    "Inquiry",
                    "Ymchwiliad",
                    "30",
                    "31",
                    "Pre-examination",
                    datetime(2025, 1, 3, 0, 0, 0, 0).isoformat(),
                    None,
                    "row-102-dupe",
                    None,
                    "message-102-dupe",
                )
            ],
            schema=T.StructType(
                [
                    T.StructField("ID", T.StringType(), True),
                    T.StructField("CaseReference", T.StringType(), True),
                    T.StructField("DisplayNameEnglish", T.StringType(), True),
                    T.StructField("DisplayNameWelsh", T.StringType(), True),
                    T.StructField("ParentFolderID", T.StringType(), True),
                    T.StructField("CaseNodeId", T.StringType(), True),
                    T.StructField("caseStage", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("RowID", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                    T.StructField("message_id", T.StringType(), True),
                ]
            ),
        )

    def generate_curated_data_schema(self):
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

    def generate_curated_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        return spark.createDataFrame(
            data=(
                (100, "APP/100", "Core Documents", "Dogfennau Craidd", 10, "decision"),
                (101, "APP/101", "Case Admin", "Gweinyddu Achos", None, "post_decision"),
                (102, "APP/102", "Inquiry", "Ymchwiliad", 30, "pre-examination"),
            ),
            schema=self.generate_curated_data_schema(),
        )

    def test__appeal_folder_curation_process__run__with_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            "appeals_folder",
            "odw_harmonised_db",
            "odw-harmonised",
            "appeals_folder",
            "overwrite",
        )

        existing_curated_data = spark.createDataFrame(
            [
                (999, "APP/999", "Old", "Hen", None, "decision"),
            ],
            self.generate_curated_data_schema(),
        )

        self.write_existing_table(
            spark,
            existing_curated_data,
            "appeal_folder",
            "odw_curated_db",
            "odw-curated",
            "appeal_folder",
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        inst = AppealFolderCurationProcess(spark)
        result = inst.run(
            orchestration_run_id="t_afcp_r_wed",
            orchestration_entity_name="appeal_folder",
            orchestration_stage_name="curate",
        )
        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_folder")
        assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__appeal_folder_curation_process__run__with_no_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        spark.sql("DROP TABLE IF EXISTS odw_curated_db.appeal_folder")

        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            "appeals_folder",
            "odw_harmonised_db",
            "odw-harmonised",
            "appeals_folder",
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        inst = AppealFolderCurationProcess(spark)
        result = inst.run(
            orchestration_run_id="t_afcp_r_wned",
            orchestration_entity_name="appeal_folder",
            orchestration_stage_name="curate",
        )
        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_folder")
        assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__appeal_folder_curation_process__run__maps_all_case_stage_values_and_lowercases_unmapped(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_data = spark.createDataFrame(
            data=[
                ("301", "APP/301", "Folder 1", "Ffolder 1", "1", "9", "Pre-application", None, None, "row-301", "Y", "m-301"),
                ("302", "APP/302", "Folder 2", "Ffolder 2", "2", "9", "Acceptance", None, None, "row-302", "Y", "m-302"),
                ("303", "APP/303", "Folder 3", "Ffolder 3", "3", "9", "Pre-examination", None, None, "row-303", "Y", "m-303"),
                ("304", "APP/304", "Folder 4", "Ffolder 4", "4", "9", "Examination", None, None, "row-304", "Y", "m-304"),
                ("305", "APP/305", "Folder 5", "Ffolder 5", "5", "9", "Recommendation", None, None, "row-305", "Y", "m-305"),
                ("306", "APP/306", "Folder 6", "Ffolder 6", "6", "9", "Decision", None, None, "row-306", "Y", "m-306"),
                ("307", "APP/307", "Folder 7", "Ffolder 7", "7", "9", "Post decision", None, None, "row-307", "Y", "m-307"),
                ("308", "APP/308", "Folder 8", "Ffolder 8", "8", "9", "Withdrawn", None, None, "row-308", "Y", "m-308"),
                ("309", "APP/309", "Folder 9", "Ffolder 9", "9", "10", "Some Mixed Stage", None, None, "row-309", "Y", "m-309"),
            ],
            schema=T.StructType(
                [
                    T.StructField("ID", T.StringType(), True),
                    T.StructField("CaseReference", T.StringType(), True),
                    T.StructField("DisplayNameEnglish", T.StringType(), True),
                    T.StructField("DisplayNameWelsh", T.StringType(), True),
                    T.StructField("ParentFolderID", T.StringType(), True),
                    T.StructField("CaseNodeId", T.StringType(), True),
                    T.StructField("caseStage", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("RowID", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                    T.StructField("message_id", T.StringType(), True),
                ]
            ),
        )

        self.write_existing_table(
            spark,
            harmonised_data,
            "appeals_folder",
            "odw_harmonised_db",
            "odw-harmonised",
            "appeals_folder",
            "overwrite",
        )

        inst = AppealFolderCurationProcess(spark)
        result = inst.run(
            orchestration_run_id="t_afcp_stage_map",
            orchestration_entity_name="appeal_folder",
            orchestration_stage_name="curate",
        )
        assert_etl_result_successful(result)
        actual_stage_data = spark.table("odw_curated_db.appeal_folder").select("id", "caseStage")

        expected_stage_data = spark.createDataFrame(
            [
                (301, "pre-application"),
                (302, "acceptance"),
                (303, "pre-examination"),
                (304, "examination"),
                (305, "recommendation"),
                (306, "decision"),
                (307, "post_decision"),
                (308, "withdrawn"),
                (309, "some mixed stage"),
            ],
            schema=T.StructType(
                [
                    T.StructField("id", T.IntegerType(), True),
                    T.StructField("caseStage", T.StringType(), True),
                ]
            ),
        )

        assert_dataframes_equal(expected_stage_data, actual_stage_data)
