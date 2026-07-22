import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process import (
    NsipExamTimetableHarmonisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
from datetime import datetime
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


class TestNSIPExamTimetableHarmonisation(ETLTestCase):
    def test__nsip_exam_timetable_harmonisation_process__run__explodes_events_and_sets_published(
        self,
    ):
        test_case = "t_nethp_r_aheasp"
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = spark.createDataFrame(
            [
                (
                    1,
                    "EN010001",
                    True,
                    [
                        Row(
                            eventId=100,
                            type="sb",
                            eventTitle="SB Event",
                            eventTitleWelsh="",
                            description="SB Desc",
                            descriptionWelsh=None,
                            date="2025-01-10 00:00:00",
                            eventDeadlineStartDate="2025-01-11 00:00:00",
                        )
                    ],
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
                                    T.StructField("eventTitle", T.StringType(), True),
                                    T.StructField(
                                        "eventTitleWelsh", T.StringType(), True
                                    ),
                                    T.StructField("description", T.StringType(), True),
                                    T.StructField(
                                        "descriptionWelsh", T.StringType(), True
                                    ),
                                    T.StructField("date", T.StringType(), True),
                                    T.StructField(
                                        "eventDeadlineStartDate", T.StringType(), True
                                    ),
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
        self.write_existing_table(
            spark,
            service_bus_data,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

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
        self.write_existing_table(
            spark,
            horizon_data,
            horizon_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_table,
            "overwrite",
        )

        horizon_nsip_data = spark.createDataFrame(
            [("EN010002", "Published", datetime(2025, 1, 1))],
            ["CaseReference", "ExamTimetablePublishStatus", "ingested_datetime"],
        )
        horizon_nsip_table = f"{test_case}_horizon_nsip_data"
        self.write_existing_table(
            spark,
            horizon_nsip_data,
            horizon_nsip_table,
            "odw_standardised_db",
            "odw-standardised",
            horizon_nsip_table,
            "overwrite",
        )
        output_table = f"{test_case}_nsip_exam_timetable"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipExamTimetableHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                NsipExamTimetableHarmonisationProcess,
                "HORIZON_TABLE",
                f"odw_standardised_db.{horizon_table}",
            ),
            mock.patch.object(
                NsipExamTimetableHarmonisationProcess,
                "HORIZON_NSIP_DATA_TABLE",
                f"odw_standardised_db.{horizon_nsip_table}",
            ),
            mock.patch.object(
                NsipExamTimetableHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = NsipExamTimetableHarmonisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_exam_timetable",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        all_rows = actual_df.collect()

        assert actual_df.count() == 3
        assert "SourceSystemID" in actual_df.columns
        assert "events" not in actual_df.columns

        rows_en010001 = [r for r in all_rows if r["caseReference"] == "EN010001"]
        rows_en010002 = [r for r in all_rows if r["caseReference"] == "EN010002"]

        assert len(rows_en010001) == 1
        assert rows_en010001[0]["Migrated"] == "1"
        assert rows_en010001[0]["eventId"] == 100
        assert (
            rows_en010001[0]["eventTitleWelsh"] is None
        )  # empty string converted to null

        assert len(rows_en010002) == 2
        assert all(r["Migrated"] == "0" for r in rows_en010002)
        assert all(r["published"] is True for r in rows_en010002)
        assert set(r["eventId"] for r in rows_en010002) == {200, 201}
