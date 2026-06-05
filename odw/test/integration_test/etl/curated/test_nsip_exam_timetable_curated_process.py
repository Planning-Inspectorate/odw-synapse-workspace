import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process import NsipExamTimetableCuratedProcess
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
import mock
import os


def _abs_format_to_adls_path(inst, container_name: str, blob_path: str, **kwargs) -> str:
    return os.path.abspath(os.path.join(container_name, blob_path))


class TestNSIPExamTimetableCurated(ETLTestCase):
    def test__nsip_exam_timetable_curated_process__run__keeps_only_projects_in_curated_project_table(self):
        test_case = "t_netcp_r_kopicpt"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_exam_timetable = spark.createDataFrame(
            [
                ("EN010001", True, 1, "Deadline", "Old Event", None, "Old desc", None, "2025-01-01 00:00", "2025-01-05 00:00", "2025-01-01 00:00:00", "Horizon"),
                ("EN010001", False, 2, "Hearing", "New Event", None, "New desc", None, "2025-02-01 00:00", "2025-02-05 00:00", "2025-02-01 00:00:00", "Horizon"),
                ("EN010001", True, 3, "Other", "SB Event", None, "SB desc", None, "2025-03-01 00:00", "2025-03-05 00:00", "2025-03-01 00:00:00", "ODT"),
                ("EN010002", True, 4, "Deadline", "Other Event", None, "Other desc", None, "2025-01-15 00:00", "2025-01-20 00:00", "2025-01-15 00:00:00", "Horizon"),
            ],
            T.StructType(
                [
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("published", T.BooleanType(), True),
                    T.StructField("eventId", T.IntegerType(), True),
                    T.StructField("type", T.StringType(), True),
                    T.StructField("eventTitle", T.StringType(), True),
                    T.StructField("eventTitleWelsh", T.StringType(), True),
                    T.StructField("description", T.StringType(), True),
                    T.StructField("descriptionWelsh", T.StringType(), True),
                    T.StructField("eventDate", T.StringType(), True),
                    T.StructField("eventDeadlineStartDate", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ODTSourceSystem", T.StringType(), True),
                ]
            ),
        )
        harmonised_exam_timetable_table = f"{test_case}_nsip_exam_timetable"

        curated_projects = spark.createDataFrame(
            [("EN010001",)],
            T.StructType([T.StructField("caseReference", T.StringType(), True)]),
        )
        curated_projects_table = f"{test_case}_nsip_project"

        self.write_existing_table(
            spark,
            harmonised_exam_timetable,
            harmonised_exam_timetable_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_exam_timetable_table,
            "overwrite",
        )
        self.write_existing_table(
            spark, curated_projects, curated_projects_table, "odw_curated_db", "odw-curated", curated_projects_table, "overwrite"
        )
        output_table = f"{test_case}_nsip_exam_timetable"
        spark.sql(f"DROP TABLE IF EXISTS odw_curated_db.{output_table}")

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(NsipExamTimetableCuratedProcess, "HARMONISED_TABLE", f"odw_harmonised_db.{harmonised_exam_timetable_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "CURATED_PROJECT_TABLE", f"odw_curated_db.{curated_projects_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "OUTPUT_TABLE", output_table),
            mock.patch.object(SynapseTableDataIO, "_format_to_adls_path", _abs_format_to_adls_path),
        ):
            inst = NsipExamTimetableCuratedProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="nsip_exam_timetable", orchestration_stage_name="curate")
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{output_table}")
        rows = actual_df.collect()

        assert actual_df.count() == 1
        assert rows[0]["caseReference"] == "EN010001"
        assert rows[0]["published"] is False
        assert rows[0]["eventId"] == 2
        assert "events" not in actual_df.columns
        assert "eventDate" in actual_df.columns
        assert "eventDeadlineStartDate" in actual_df.columns
