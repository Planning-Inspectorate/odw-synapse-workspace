from datetime import datetime

import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T
import pytest
from odw.core.etl.transformation.curated.legacy_folder_data_curation_process import LegacyFolderDataCurationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


class TestLegacyFolderDataCurationProcess(ETLTestCase):
    ACTIVE_HARMONISED_DATA = [
        ("100", "LFD/100", "Core Docs", "Core Docs CY", "10", "11", "Decision", datetime(2025, 1, 1).isoformat(), None, "row-100", "Y", "m-100"),
        ("101", "LFD/101", "Admin", "Admin CY", "20", "20", "Post decision", datetime(2025, 1, 2).isoformat(), None, "row-101", "N", "m-101"),
        ("102", "LFD/102", "Inquiry", "Inquiry CY", "30", "31", "Pre-examination", datetime(2025, 1, 3).isoformat(), None, "row-102", None, "m-102"),
    ]

    def generate_harmonised_table(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        return spark.createDataFrame(
            data=self.ACTIVE_HARMONISED_DATA
            + [
                (
                    "102",
                    "LFD/102",
                    "Inquiry",
                    "Inquiry CY",
                    "30",
                    "31",
                    "Pre-examination",
                    datetime(2025, 1, 3).isoformat(),
                    None,
                    "row-102-dupe",
                    None,
                    "m-102-dupe",
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
                (100, "LFD/100", "Core Docs", "Core Docs CY", 10, "decision"),
                (101, "LFD/101", "Admin", "Admin CY", None, "post_decision"),
                (102, "LFD/102", "Inquiry", "Inquiry CY", 30, "pre-examination"),
            ),
            schema=self.generate_curated_data_schema(),
        )

    def test__legacy_folder_data_curation_process__run__with_existing_data(self):
        test_case = "t_lfdcp_r_wed"
        harmonised_table = f"{test_case}_horizon_folder"
        curated_table = f"{test_case}_legacy_folder_data"
        spark = PytestSparkSessionUtil().get_spark_session()

        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        existing_curated_data = spark.createDataFrame(
            [(999, "LFD/999", "Old", "Old CY", None, "decision")],
            self.generate_curated_data_schema(),
        )

        self.write_existing_table(
            spark,
            existing_curated_data,
            curated_table,
            "odw_curated_db",
            "odw-curated",
            curated_table,
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        with mock.patch.object(
            LegacyFolderDataCurationProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{harmonised_table}",
        ), mock.patch.object(LegacyFolderDataCurationProcess, "OUTPUT_TABLE", curated_table):
            inst = LegacyFolderDataCurationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="legacy_folder_data",
                orchestration_stage_name="curate",
            )
        assert_etl_result_successful(result)
        actual_table_data = spark.table(f"odw_curated_db.{curated_table}")
        assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__legacy_folder_data_curation_process__run__with_no_existing_data(self):
        test_case = "t_lfdcp_r_wned"
        harmonised_table = f"{test_case}_horizon_folder"
        curated_table = f"{test_case}_legacy_folder_data"
        spark = PytestSparkSessionUtil().get_spark_session()

        spark.sql(f"DROP TABLE IF EXISTS odw_curated_db.{curated_table}")

        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        with mock.patch.object(
            LegacyFolderDataCurationProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{harmonised_table}",
        ), mock.patch.object(LegacyFolderDataCurationProcess, "OUTPUT_TABLE", curated_table):
            inst = LegacyFolderDataCurationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="legacy_folder_data",
                orchestration_stage_name="curate",
            )
        assert_etl_result_successful(result)
        actual_table_data = spark.table(f"odw_curated_db.{curated_table}")
        assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__legacy_folder_data_curation_process__run__maps_all_case_stage_values_and_lowercases_unmapped(self):
        test_case = "t_lfdcp_stage_map"
        harmonised_table = f"{test_case}_horizon_folder"
        curated_table = f"{test_case}_legacy_folder_data"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_data = spark.createDataFrame(
            data=[
                ("301", "LFD/301", "Folder 1", "Folder 1 CY", "1", "9", "Pre-application", None, None, "row-301", "Y", "m-301"),
                ("302", "LFD/302", "Folder 2", "Folder 2 CY", "2", "9", "Acceptance", None, None, "row-302", "Y", "m-302"),
                ("303", "LFD/303", "Folder 3", "Folder 3 CY", "3", "9", "Pre-examination", None, None, "row-303", "Y", "m-303"),
                ("304", "LFD/304", "Folder 4", "Folder 4 CY", "4", "9", "Examination", None, None, "row-304", "Y", "m-304"),
                ("305", "LFD/305", "Folder 5", "Folder 5 CY", "5", "9", "Recommendation", None, None, "row-305", "Y", "m-305"),
                ("306", "LFD/306", "Folder 6", "Folder 6 CY", "6", "9", "Decision", None, None, "row-306", "Y", "m-306"),
                ("307", "LFD/307", "Folder 7", "Folder 7 CY", "7", "9", "Post decision", None, None, "row-307", "Y", "m-307"),
                ("308", "LFD/308", "Folder 8", "Folder 8 CY", "8", "9", "Withdrawn", None, None, "row-308", "Y", "m-308"),
                ("309", "LFD/309", "Folder 9", "Folder 9 CY", "9", "10", "Some Mixed Stage", None, None, "row-309", "Y", "m-309"),
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
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )

        with mock.patch.object(
            LegacyFolderDataCurationProcess,
            "HARMONISED_TABLE",
            f"odw_harmonised_db.{harmonised_table}",
        ), mock.patch.object(LegacyFolderDataCurationProcess, "OUTPUT_TABLE", curated_table):
            inst = LegacyFolderDataCurationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="legacy_folder_data",
                orchestration_stage_name="curate",
            )
        assert_etl_result_successful(result)
        actual_stage_data = spark.table(f"odw_curated_db.{curated_table}").select("id", "caseStage")

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
