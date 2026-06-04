import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process import NsipExamTimetableCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


class TestNSIPExamTimetableCurated(ETLTestCase):
    def test__nsip_exam_timetable_curated_process__run__keeps_only_projects_in_curated_project_table(self):
        test_case = "t_netcp_r_kopicpt"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_exam_timetable = spark.createDataFrame(
            [
                ("EN010001", True, [Row(eventId=1, name="Old Horizon Event")], "2025-01-01 00:00:00", "Horizon"),
                ("EN010001", False, [Row(eventId=2, name="New Horizon Event")], "2025-02-01 00:00:00", "Horizon"),
                ("EN010001", True, [Row(eventId=3, name="Service Bus Event")], "2025-03-01 00:00:00", "ODT"),
                ("EN010002", True, [Row(eventId=4, name="Other Horizon Event")], "2025-01-15 00:00:00", "Horizon"),
            ],
            T.StructType(
                [
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("published", T.BooleanType(), True),
                    T.StructField(
                        "events",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("eventId", T.IntegerType(), True),
                                    T.StructField("name", T.StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
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

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(NsipExamTimetableCuratedProcess, "HARMONISED_TABLE", f"odw_harmonised_db.{harmonised_exam_timetable_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "CURATED_PROJECT_TABLE", f"odw_curated_db.{curated_projects_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "OUTPUT_TABLE", output_table),
        ):
            inst = NsipExamTimetableCuratedProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="nsip_exam_timetable", orchestration_stage_name="curate")
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{output_table}")
        rows = actual_df.collect()

        assert actual_df.count() == 1
        assert rows[0]["caseReference"] == "EN010001"
        assert rows[0]["published"] is False
        assert len(rows[0]["events"]) == 1
        assert rows[0]["events"][0]["eventId"] == 2
