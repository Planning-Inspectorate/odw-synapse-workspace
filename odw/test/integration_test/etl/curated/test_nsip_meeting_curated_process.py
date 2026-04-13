import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_meeting_curated_process import NsipMeetingCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


class NSIPMeetingCuratedTestCase(ETLTestCase):
    def test__nsip_meeting_curated_process__run__keeps_latest_row_per_meeting_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_meeting = spark.createDataFrame(
            [
                (100, "EN010001", "agenda-old", "role-1", "M-1", "2025-01-01", "type-a", "2025-01-10", "Y"),
                (100, "EN010001", "agenda-new", "role-1", "M-1", "2025-01-05", "type-a", "2025-01-12", "Y"),
                (200, "EN010002", "agenda-2", "role-2", "M-2", "2025-02-01", "type-b", "2025-02-11", "Y"),
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
                    T.StructField("estimatedPrelimMeetingDate", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        source_data = {
            "harmonised_meeting": harmonised_meeting,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_meeting_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.nsip_meeting_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = NsipMeetingCuratedProcess(spark)

            with mock.patch.object(inst, "load_data", return_value=source_data), mock.patch.object(inst, "write_data") as mock_write:
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = {row["meetingId"]: row.asDict(recursive=True) for row in actual_df.collect()}

        assert actual_df.count() == 2
        assert rows["M-1"]["meetingAgenda"] == "agenda-new"
        assert rows["M-1"]["meetingDate"] == "2025-01-05"
        assert rows["M-2"]["meetingAgenda"] == "agenda-2"

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_meeting"
        assert result.metadata.insert_count == 2
