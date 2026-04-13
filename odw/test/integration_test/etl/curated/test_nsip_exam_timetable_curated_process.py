import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process import NsipExamTimetableCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


class NSIPExamTimetableCuratedTestCase(ETLTestCase):
    def test__nsip_exam_timetable_curated_process__run__keeps_only_projects_in_curated_project_table(self):
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

        curated_projects = spark.createDataFrame(
            [("EN010001",)],
            T.StructType([T.StructField("caseReference", T.StringType(), True)]),
        )

        source_data = {
            "harmonised_exam_timetable": harmonised_exam_timetable,
            "curated_projects": curated_projects,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = NsipExamTimetableCuratedProcess(spark)

            with mock.patch.object(inst, "load_data", return_value=source_data), mock.patch.object(inst, "write_data") as mock_write:
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = actual_df.collect()

        assert actual_df.count() == 1
        assert rows[0]["caseReference"] == "EN010001"
        assert rows[0]["published"] is False
        assert len(rows[0]["events"]) == 1
        assert rows[0]["events"][0]["eventId"] == 2

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_exam_timetable"
        assert result.metadata.insert_count == 1
