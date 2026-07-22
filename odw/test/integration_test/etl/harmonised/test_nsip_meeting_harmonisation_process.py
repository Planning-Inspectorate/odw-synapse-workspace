import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process import (
    NsipMeetingHarmonisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from pyspark.sql import Row
import pyspark.sql.types as T
import hashlib
import mock


class TestNSIPMeetingHarmonisation(ETLTestCase):
    def test__nsip_meeting_harmonisation_process__run__initial_load_overwrites_and_keeps_latest_per_business_key(
        self,
    ):
        test_case = "t_nmhp_r_iloaklpbk"
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = spark.createDataFrame(
            [
                (
                    1,
                    100,
                    "EN010001",
                    [
                        Row(
                            meetingId="M-1",
                            meetingAgenda="old",
                            planningInspectorateRole="role",
                            meetingDate="2025-01-01",
                            meetingType="type-a",
                        )
                    ],
                    "1",
                    "ODT",
                    "SRC1",
                    "2025-01-01 00:00:00",
                ),
                (
                    1,
                    100,
                    "EN010001",
                    [
                        Row(
                            meetingId="M-1",
                            meetingAgenda="new",
                            planningInspectorateRole="role",
                            meetingDate="2025-01-02",
                            meetingType="type-a",
                        )
                    ],
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
        service_bus_table = f"{test_case}_sb_nsip_project"
        self.write_existing_table(
            spark,
            service_bus_data,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

        output_table = f"{test_case}_sb_nsip_meeting"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = NsipMeetingHarmonisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_meeting",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        rows = [row.asDict(recursive=True) for row in actual_df.collect()]

        assert len(rows) == 2

        active_rows = [row for row in rows if row["IsActive"] == "Y"]
        inactive_rows = [row for row in rows if row["IsActive"] == "N"]

        assert len(active_rows) == 1
        assert len(inactive_rows) == 1

        assert active_rows[0]["meetingId"] == "M-1"
        assert active_rows[0]["meetingAgenda"] == "new"
        assert active_rows[0]["ValidTo"] is None

        assert inactive_rows[0]["meetingId"] == "M-1"
        assert inactive_rows[0]["meetingAgenda"] == "old"
        assert inactive_rows[0]["ValidTo"] is not None
        assert str(inactive_rows[0]["ValidTo"]).startswith("2025-01-02")

    def test__nsip_meeting_harmonisation_process__run__incremental_change_expires_old_record_and_inserts_new_version(
        self,
    ):
        test_case = "t_nmhp_r_iceorainv"
        spark = PytestSparkSessionUtil().get_spark_session()

        service_bus_data = spark.createDataFrame(
            [
                (
                    1,
                    100,
                    "EN010001",
                    [
                        Row(
                            meetingId="M-1",
                            meetingAgenda="changed",
                            planningInspectorateRole="role",
                            meetingDate="2025-01-03",
                            meetingType="type-a",
                        )
                    ],
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

        service_bus_table = f"{test_case}_sb_nsip_project"
        self.write_existing_table(
            spark,
            service_bus_data,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

        output_table = f"{test_case}_sb_nsip_meeting"
        self.write_existing_table(
            spark,
            target_df,
            output_table,
            "odw_harmonised_db",
            "odw-harmonised",
            output_table,
            "overwrite",
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = NsipMeetingHarmonisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_meeting",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        rows = [row.asDict(recursive=True) for row in actual_df.collect()]

        assert len(rows) == 2

        inactive_rows = [row for row in rows if row["IsActive"] == "N"]
        active_rows = [row for row in rows if row["IsActive"] == "Y"]

        assert len(inactive_rows) == 1
        assert len(active_rows) == 1

        assert inactive_rows[0]["meetingAgenda"] == "old"
        assert inactive_rows[0]["ValidTo"] is not None
        assert str(inactive_rows[0]["ValidTo"]).startswith("2025-01-03")

        assert active_rows[0]["meetingAgenda"] == "changed"
        assert active_rows[0]["ValidTo"] is None

    def test__nsip_meeting_harmonisation_process__run__nsip_project_info_reallocation_does_not_create_new_version(
        self,
    ):
        test_case = "t_nmhp_r_npiradncnv"
        spark = PytestSparkSessionUtil().get_spark_session()
        matching_row_hash = hashlib.sha256(
            "100~M-1~same~role~type-a~2025-01-01".encode("utf-8")
        ).hexdigest()

        service_bus_data = spark.createDataFrame(
            [
                (
                    999,
                    100,
                    "EN010001",
                    [
                        Row(
                            meetingId="M-1",
                            meetingAgenda="same",
                            planningInspectorateRole="role",
                            meetingDate="2025-01-01",
                            meetingType="type-a",
                        )
                    ],
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
                    "same",
                    "role",
                    "2025-01-01",
                    "type-a",
                    "1",
                    "ODT",
                    "SRC1",
                    "2025-01-01 00:00:00",
                    matching_row_hash,
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

        service_bus_table = f"{test_case}_sb_nsip_project"
        self.write_existing_table(
            spark,
            service_bus_data,
            service_bus_table,
            "odw_harmonised_db",
            "odw-harmonised",
            service_bus_table,
            "overwrite",
        )

        output_table = f"{test_case}_sb_nsip_meeting"
        self.write_existing_table(
            spark,
            target_df,
            output_table,
            "odw_harmonised_db",
            "odw-harmonised",
            output_table,
            "overwrite",
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess,
                "SERVICE_BUS_TABLE",
                f"odw_harmonised_db.{service_bus_table}",
            ),
            mock.patch.object(
                NsipMeetingHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = NsipMeetingHarmonisationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_meeting",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_harmonised_db.{output_table}")
        actual_subset_df = actual_df.select("IsActive", "ValidTo", "meetingAgenda")
        expected_subset_df = spark.createDataFrame(
            [("Y", None, "same")], actual_subset_df.schema
        )

        assert_dataframes_equal(expected_subset_df, actual_subset_df)
