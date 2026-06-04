import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process import NsipExamTimetableHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
from datetime import datetime
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


class TestNSIPExamTimetableHarmonisation(ETLTestCase):
    def test__nsip_exam_timetable_harmonisation_process__run__aggregates_horizon_events_and_sets_published(self):
        test_case = "t_nethp_r_aheasp"
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = spark.createDataFrame(
            [
                (
                    1,
                    "EN010001",
                    True,
                    [Row(eventId=100, type="sb")],
                    "1",
                    "ODT",
                    "SRC1",
                    "2025-01-01 00:00:00",
                    None,
                    "",
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("NSIPExaminationTimetableID", T.IntegerType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("published", T.BooleanType(), True),
                    T.StructField(
                        "events",
                        T.ArrayType(
                            T.StructType(
                                [
                                    T.StructField("eventId", T.IntegerType(), True),
                                    T.StructField("type", T.StringType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                    T.StructField("Migrated", T.StringType(), True),
                    T.StructField("ODTSourceSystem", T.StringType(), True),
                    T.StructField("SourceSystemID", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("RowID", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )
        service_bus_table = f"{test_case}_sb_nsip_exam_timetable"
        self.write_existing_table(spark, service_bus_data, service_bus_table, "odw_harmonised_db", "odw-harmonised", service_bus_table, "overwrite")

        horizon_data = spark.createDataFrame(
            [
                (
                    "EN010002",
                    200,
                    "Deadline",
                    "Title 1",
                    "Title 1 CY",
                    "Desc 1",
                    "Desc 1 CY",
                    "2025-02-10",
                    "2025-02-11",
                    "2025-02-01 00:00:00",
                    datetime(2025, 1, 1),
                ),
                (
                    "EN010002",
                    201,
                    "Hearing",
                    "Title 2",
                    "Title 2 CY",
                    "Desc 2",
                    "Desc 2 CY",
                    "2025-02-12",
                    "2025-02-13",
                    "2025-02-01 00:00:00",
                    datetime(2025, 1, 1),
                ),
            ],
            [
                "CaseReference",
                "ID",
                "typeofexamination",
                "Name",
                "NameWelsh",
                "Description",
                "DescriptionWelsh",
                "DeadlineStartDateTime",
                "Date",
                "expected_from",
                "ingested_datetime",
            ],
        )
        horizon_table = f"{test_case}_horizon_examination_timetable"
        self.write_existing_table(spark, horizon_data, horizon_table, "odw_standardised_db", "odw-standardised", horizon_table, "overwrite")

        horizon_nsip_data = spark.createDataFrame(
            [("EN010002", "Published", datetime(2025, 1, 1))],
            ["CaseReference", "ExamTimetablePublishStatus", "ingested_datetime"],
        )
        horizon_nsip_table = f"{test_case}_horizon_nsip_data"
        self.write_existing_table(
            spark, horizon_nsip_data, horizon_nsip_table, "odw_standardised_db", "odw-standardised", horizon_nsip_table, "overwrite"
        )
        output_table = f"{test_case}_nsip_exam_timetable"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(NsipExamTimetableHarmonisationProcess, "SERVICE_BUS_TABLE", f"odw_harmonised_db.{service_bus_table}"),
            mock.patch.object(NsipExamTimetableHarmonisationProcess, "HORIZON_TABLE", f"odw_standardised_db.{horizon_table}"),
            mock.patch.object(NsipExamTimetableHarmonisationProcess, "HORIZON_NSIP_DATA_TABLE", f"odw_standardised_db.{horizon_nsip_table}"),
            mock.patch.object(NsipExamTimetableHarmonisationProcess, "OUTPUT_TABLE", output_table),
        ):
            inst = NsipExamTimetableHarmonisationProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="nsip_exam_timetable", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        rows = {row["caseReference"]: row.asDict(recursive=True) for row in actual_df.collect()}

        assert actual_df.count() == 2
        assert "SourceSystemID" not in actual_df.columns
        assert "SourceSystemID" not in rows["EN010001"]
        assert "SourceSystemID" not in rows["EN010002"]
        assert rows["EN010001"]["Migrated"] == "1"
        assert rows["EN010002"]["Migrated"] == "0"
        assert rows["EN010002"]["published"] is True
        assert len(rows["EN010002"]["events"]) == 2
