from odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process import NsipExamTimetableHarmonisationProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


class TestNSIPExamTimetableHarmonisationProcess(SparkTestCase):
    def test__nsip_exam_timetable_harmonisation_process__process__explodes_events_and_sets_published(self):
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
                                    T.StructField("eventTitleWelsh", T.StringType(), True),
                                    T.StructField("description", T.StringType(), True),
                                    T.StructField("descriptionWelsh", T.StringType(), True),
                                    T.StructField("date", T.StringType(), True),
                                    T.StructField("eventDeadlineStartDate", T.StringType(), True),
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
                    "2025-02-10 00:00",
                    "2025-02-11 00:00",
                    "2025-02-01 00:00:00",
                ),
                (
                    "EN010002",
                    201,
                    "Hearing",
                    "Title 2",
                    "Title 2 CY",
                    "Desc 2",
                    "Desc 2 CY",
                    "2025-02-12 00:00",
                    "2025-02-13 00:00",
                    "2025-02-01 00:00:00",
                ),
            ],
            [
                "CaseReference",
                "eventId",
                "type",
                "eventTitle",
                "eventTitleWelsh",
                "description",
                "descriptionWelsh",
                "eventDate",
                "eventDeadlineStartDate",
                "expected_from",
            ],
        )

        horizon_nsip_data = spark.createDataFrame(
            [("EN010002", "Published")],
            ["CaseReference", "ExamTimetablePublishStatus"],
        )

        sb_case_references = spark.createDataFrame(
            [("EN010001",)],
            ["caseReference"],
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.LoggingUtil"),
        ):
            inst = NsipExamTimetableHarmonisationProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "service_bus_data": service_bus_data,
                    "horizon_data": horizon_data,
                    "horizon_nsip_data": horizon_nsip_data,
                    "sb_case_references": sb_case_references,
                }
            )
        write_entry_key = f"odw_harmonised_db.{inst.OUTPUT_TABLE}"

        actual_df = data_to_write[write_entry_key]["data"]
        all_rows = actual_df.collect()

        assert actual_df.count() == 3
        assert "SourceSystemID" in actual_df.columns
        assert "events" not in actual_df.columns

        rows_en010001 = [r for r in all_rows if r["caseReference"] == "EN010001"]
        rows_en010002 = [r for r in all_rows if r["caseReference"] == "EN010002"]

        assert len(rows_en010001) == 1
        assert rows_en010001[0]["Migrated"] == "1"
        assert rows_en010001[0]["eventId"] == 100
        assert rows_en010001[0]["eventTitleWelsh"] is None  # empty string converted to null

        assert len(rows_en010002) == 2
        assert all(r["Migrated"] == "0" for r in rows_en010002)
        assert all(r["published"] is True for r in rows_en010002)
        assert set(r["eventId"] for r in rows_en010002) == {200, 201}

        assert data_to_write[write_entry_key]["write_mode"] == "overwrite"
        assert data_to_write[write_entry_key]["partition_by"] == ["IsActive"]
        assert result.metadata.insert_count == 3
