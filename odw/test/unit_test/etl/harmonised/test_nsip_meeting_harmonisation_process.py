from odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process import NsipMeetingHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


def test__nsip_meeting_harmonisation_process__process__initial_load_overwrites_and_keeps_latest_per_business_key():
    spark = PytestSparkSessionUtil().get_spark_session()

    service_bus_data = spark.createDataFrame(
        [
            (
                1,
                100,
                "EN010001",
                [Row(meetingId="M-1", meetingAgenda="old", planningInspectorateRole="role", meetingDate="2025-01-01", meetingType="type-a", estimatedPrelimMeetingDate="2025-01-10")],
                "1",
                "ODT",
                "SRC1",
                "2025-01-01 00:00:00",
            ),
            (
                1,
                100,
                "EN010001",
                [Row(meetingId="M-1", meetingAgenda="new", planningInspectorateRole="role", meetingDate="2025-01-02", meetingType="type-a", estimatedPrelimMeetingDate="2025-01-11")],
                "1",
                "ODT",
                "SRC1",
                "2025-01-02 00:00:00",
            ),
        ],
        [
            "NSIPProjectInfoInternalID",
            "caseId",
            "caseReference",
            "meetings",
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
        ],
    )

    with mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
    ), mock.patch(
        "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.LoggingUtil"
    ):
        inst = NsipMeetingHarmonisationProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "service_bus_data": service_bus_data,
                "target_df": None,
            }
        )

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = actual_df.collect()

    assert actual_df.count() == 1
    assert rows[0]["meetingId"] == "M-1"
    assert rows[0]["meetingAgenda"] == "new"
    assert rows[0]["IsActive"] == "Y"
    assert rows[0]["ValidTo"] is None

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
    assert result.metadata.update_count == 0


def test__nsip_meeting_harmonisation_process__process__incremental_change_expires_old_record_and_inserts_new_version():
    spark = PytestSparkSessionUtil().get_spark_session()

    service_bus_data = spark.createDataFrame(
        [
            (
                1,
                100,
                "EN010001",
                [Row(meetingId="M-1", meetingAgenda="changed", planningInspectorateRole="role", meetingDate="2025-01-03", meetingType="type-a", estimatedPrelimMeetingDate="2025-01-12")],
                "1",
                "ODT",
                "SRC1",
                "2025-01-03 00:00:00",
            ),
        ],
        [
            "NSIPProjectInfoInternalID",
            "caseId",
            "caseReference",
            "meetings",
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
        ],
    )

    target_df = spark.createDataFrame(
        [
            (
                1,
                1,
                100,
                "EN010001",
                "M-1",
                "old",
                "role",
                "2025-01-01",
                "type-a",
                "2025-01-10",
                "1",
                "ODT",
                "SRC1",
                "2025-01-01 00:00:00",
                "existing_hash",
                None,
                "Y",
            ),
        ],
        T.StructType(
            [
                T.StructField("NSIPMeetingId", T.IntegerType(), True),
                T.StructField("NSIPProjectInfoInternalID", T.IntegerType(), True),
                T.StructField("caseId", T.IntegerType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("meetingId", T.StringType(), True),
                T.StructField("meetingAgenda", T.StringType(), True),
                T.StructField("planningInspectorateRole", T.StringType(), True),
                T.StructField("meetingDate", T.StringType(), True),
                T.StructField("meetingType", T.StringType(), True),
                T.StructField("estimatedPrelimMeetingDate", T.StringType(), True),
                T.StructField("Migrated", T.StringType(), True),
                T.StructField("ODTSourceSystem", T.StringType(), True),
                T.StructField("SourceSystemID", T.StringType(), True),
                T.StructField("IngestionDate", T.StringType(), True),
                T.StructField("row_hash", T.StringType(), True),
                T.StructField("ValidTo", T.TimestampType(), True),
                T.StructField("IsActive", T.StringType(), True),
            ]
        ),
    )

    with mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
    ), mock.patch(
        "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.LoggingUtil"
    ):
        inst = NsipMeetingHarmonisationProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "service_bus_data": service_bus_data,
                "target_df": target_df,
            }
        )

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = [row.asDict(recursive=True) for row in actual_df.collect()]

    assert len(rows) == 2

    inactive_rows = [row for row in rows if row["IsActive"] == "N"]
    active_rows = [row for row in rows if row["IsActive"] == "Y"]

    assert len(inactive_rows) == 1
    assert len(active_rows) == 1

    assert inactive_rows[0]["meetingAgenda"] == "old"
    assert inactive_rows[0]["ValidTo"] is not None

    assert active_rows[0]["meetingAgenda"] == "changed"
    assert active_rows[0]["ValidTo"] is None

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert result.metadata.insert_count == 1
    assert result.metadata.update_count == 1