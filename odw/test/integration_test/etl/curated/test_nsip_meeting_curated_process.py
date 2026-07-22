import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_meeting_curated_process import (
    NsipMeetingCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
import mock


class TestNSIPMeetingCurated(ETLTestCase):
    def test__nsip_meeting_curated_process__run__keeps_latest_row_per_meeting_id(self):
        test_case = "t_nmcp_r_klrpmi"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_meeting = spark.createDataFrame(
            [
                (
                    100,
                    "EN010001",
                    "agenda-old",
                    "role-1",
                    "M-1",
                    "2025-01-01",
                    "type-a",
                    "Y",
                ),
                (
                    100,
                    "EN010001",
                    "agenda-new",
                    "role-1",
                    "M-1",
                    "2025-01-05",
                    "type-a",
                    "Y",
                ),
                (
                    200,
                    "EN010002",
                    "agenda-2",
                    "role-2",
                    "M-2",
                    "2025-02-01",
                    "type-b",
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("caseId", T.IntegerType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("meetingAgenda", T.StringType(), True),
                    T.StructField("planningInspectorateRole", T.StringType(), True),
                    T.StructField("meetingId", T.StringType(), True),
                    T.StructField("meetingDate", T.StringType(), True),
                    T.StructField("meetingType", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        harmonised_meeting_table = f"{test_case}_sb_nsip_meeting"
        self.write_existing_table(
            spark,
            harmonised_meeting,
            harmonised_meeting_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_meeting_table,
            "overwrite",
        )
        expected_output_table = f"{test_case}_nsip_meeting"

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_meeting_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipMeetingCuratedProcess,
                "HARMONISED_TABLE",
                f"odw_harmonised_db.{harmonised_meeting_table}",
            ),
            mock.patch.object(
                NsipMeetingCuratedProcess, "OUTPUT_TABLE", expected_output_table
            ),
        ):
            inst = NsipMeetingCuratedProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_meeting",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{expected_output_table}")
        rows = {
            row["meetingId"]: row.asDict(recursive=True) for row in actual_df.collect()
        }

        assert actual_df.count() == 2
        assert rows["M-1"]["meetingAgenda"] == "agenda-new"
        assert rows["M-1"]["meetingDate"] == "2025-01-05"
        assert rows["M-2"]["meetingAgenda"] == "agenda-2"
