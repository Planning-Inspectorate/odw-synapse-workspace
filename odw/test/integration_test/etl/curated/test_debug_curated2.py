import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process import NsipExamTimetableCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
from pyspark.sql import functions as F
import mock


class TestDebugCurated2(ETLTestCase):
    def test_debug(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_dbg2_c"
        schema = T.StructType([
            T.StructField("caseReference", T.StringType(), True),
            T.StructField("published", T.BooleanType(), True),
            T.StructField("eventId", T.IntegerType(), True),
            T.StructField("type", T.StringType(), True),
            T.StructField("eventTitle", T.StringType(), True),
            T.StructField("eventTitleWelsh", T.StringType(), True),
            T.StructField("description", T.StringType(), True),
            T.StructField("descriptionWelsh", T.StringType(), True),
            T.StructField("eventDate", T.StringType(), True),
            T.StructField("eventDeadlineStartDate", T.StringType(), True),
            T.StructField("IngestionDate", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
        ])
        harmonised_data = spark.createDataFrame([
            ("EN010001", False, 2, "Hearing", "New Event", None, "New desc", None, "2025-02-01 00:00", "2025-02-05 00:00", "2025-02-01 00:00:00", "Horizon"),
        ], schema)
        harmonised_table = f"{test_case}_nsip_exam_timetable"
        self.write_existing_table(spark, harmonised_data, harmonised_table, "odw_harmonised_db", "odw-harmonised", harmonised_table, "overwrite")
        
        # Verify harmonised data is in the table
        check = spark.sql(f"SELECT count(*) as cnt FROM odw_harmonised_db.{harmonised_table}")
        print(f"\n=== Harmonised table count: {check.first()['cnt']}")

        proj = spark.createDataFrame([("EN010001",)], T.StructType([T.StructField("caseReference", T.StringType(), True)]))
        proj_table = f"{test_case}_nsip_project"
        self.write_existing_table(spark, proj, proj_table, "odw_curated_db", "odw-curated", proj_table, "overwrite")
        output_table = f"{test_case}_nsip_exam_timetable"

        with (
            mock.patch("odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process.Util.get_storage_account", return_value="test_storage"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "HARMONISED_TABLE", f"odw_harmonised_db.{harmonised_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "CURATED_PROJECT_TABLE", f"odw_curated_db.{proj_table}"),
            mock.patch.object(NsipExamTimetableCuratedProcess, "OUTPUT_TABLE", output_table),
        ):
            inst = NsipExamTimetableCuratedProcess(spark)
            src = inst.load_data()
            print(f"=== harmonised from load_data count: {src['harmonised_exam_timetable'].count()}")
            print(f"=== projects from load_data count: {src['curated_projects'].count()}")
            
            # Manually run process
            data_to_write, result = inst.process(source_data=src)
            print(f"=== insert_count from process: {result.metadata.insert_count}")
            out_df = data_to_write[f"odw_curated_db.{output_table}"]["data"]
            print(f"=== actual df count before write: {out_df.count()}")
            
            full_result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="nsip_exam_timetable", orchestration_stage_name="curate")
            assert_etl_result_successful(full_result)
            print(f"=== insert_count from run: {full_result.metadata.insert_count}")

        out = spark.table(f"odw_curated_db.{output_table}")
        print(f"=== output table count: {out.count()}")
        assert out.count() == 1
