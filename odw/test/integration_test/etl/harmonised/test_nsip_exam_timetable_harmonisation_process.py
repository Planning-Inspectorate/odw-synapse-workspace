import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process import NsipExamTimetableHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


def test__nsip_exam_timetable_harmonisation_process__run__aggregates_horizon_events_and_sets_published():
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

    horizon_data = spark.createDataFrame(
        [
            ("EN010002", 200, "Deadline", "Title 1", "Title 1 CY", "Desc 1", "Desc 1 CY", "2025-02-10", "2025-02-11", "2025-02-01 00:00:00"),
            ("EN010002", 201, "Hearing", "Title 2", "Title 2 CY", "Desc 2", "Desc 2 CY", "2025-02-12", "2025-02-13", "2025-02-01 00:00:00"),
        ],
        [
            "CaseReference",
            "eventId",
            "type",
            "eventTitle",
            "eventTitleWelsh",
            "description",
            "descriptionWelsh",
            "eventDeadlineStartDate",
            "date",
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

    source_data = {
        "service_bus_data": service_bus_data,
        "horizon_data": horizon_data,
        "horizon_nsip_data": horizon_nsip_data,
        "sb_case_references": sb_case_references,
    }

    with mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
    ), mock.patch(
        "odw.core.etl.etl_process.LoggingUtil"
    ) as MockEtlLogging, mock.patch(
        "odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process.LoggingUtil"
    ) as MockProcessLogging:
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipExamTimetableHarmonisationProcess(spark)

        with mock.patch.object(inst, "load_data", return_value=source_data), \
                mock.patch.object(inst, "write_data") as mock_write:
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = {row["caseReference"]: row.asDict(recursive=True) for row in actual_df.collect()}

    assert actual_df.count() == 2
    assert rows["EN010001"]["Migrated"] == "1"
    assert rows["EN010002"]["Migrated"] == "0"
    assert rows["EN010002"]["published"] is True
    assert len(rows["EN010002"]["events"]) == 2

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert data_to_write[inst.OUTPUT_TABLE]["partition_by"] == ["IsActive"]
    assert result.metadata.insert_count == 2