import uuid
from datetime import datetime
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process import HorizonPinsInspectorHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


def _horizon_schema():
    return T.StructType(
        [
            T.StructField("horizonId", T.StringType(), True),
            T.StructField("firstName", T.StringType(), True),
            T.StructField("lastName", T.StringType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IngestionDate", T.StringType(), True),
        ]
    )


def _hrm_schema():
    return T.StructType(
        [
            T.StructField("horizonId", T.StringType(), True),
            T.StructField("firstName", T.StringType(), True),
            T.StructField("lastName", T.StringType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("Migrated", T.StringType(), True),
            T.StructField("ODTSourceSystem", T.StringType(), True),
            T.StructField("ValidTo", T.TimestampType(), True),
            T.StructField("IsActive", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
        ]
    )


class TestHorizonPinsInspectorHarmonisationProcess(ETLTestCase):
    def setup_method(self):
        self.test_suffix = uuid.uuid4().hex
        spark = PytestSparkSessionUtil().get_spark_session()
        spark.sql(f"DROP TABLE IF EXISTS {HorizonPinsInspectorHarmonisationProcess.STAGE_TABLE}")
        spark.sql(f"DROP TABLE IF EXISTS {HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE}")

    def _write_horizon(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _horizon_schema()),
            "horizon_pins_inspector",
            "odw_standardised_db",
            "odw-standardised",
            f"horizon_pins_inspector/{self.test_suffix}",
            "overwrite",
        )

    def test__run__builds_scd2_timeline_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_horizon(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),
                ("HZN-002", "Bob", "Jones", "", "2024-03-01 10:00:00"),
            ],
        )

        result = HorizonPinsInspectorHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        assert result.metadata.insert_count == 3
        assert result.metadata.update_count == 0
        assert result.metadata.delete_count == 0

        actual = spark.table(HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE)
        assert actual.columns == [field.name for field in _hrm_schema()]

        actual_df = actual.select("horizonId", "lastName", "IngestionDate", "ValidTo", "IsActive", "ODTSourceSystem").orderBy(
            "horizonId", "IngestionDate"
        )
        expected_df = spark.createDataFrame(
            [
                ("HZN-001", "Smith", datetime(2024, 1, 1, 10), datetime(2024, 6, 1, 10), "N", "HORIZON"),
                ("HZN-001", "Brown", datetime(2024, 6, 1, 10), None, "Y", "HORIZON"),
                ("HZN-002", "Jones", datetime(2024, 3, 1, 10), None, "Y", "HORIZON"),
            ],
            actual_df.schema,
        )
        assert_dataframes_equal(actual_df, expected_df)

    def test__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_horizon(spark)

        result = HorizonPinsInspectorHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE)
        assert actual.count() == 0
        assert result.metadata.insert_count == 0

    def test__run__deduplication_null_filtering_and_scd2_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_horizon(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),  # exact duplicate
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),  # state change
                (None, "Bad", "Row", "", "2024-01-01 10:00:00"),  # null key — filtered
            ],
        )

        result = HorizonPinsInspectorHarmonisationProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE)
        assert result.metadata.insert_count == 2
        assert actual.where(F.col("horizonId").isNull()).count() == 0
        assert actual.where(F.col("IsActive") == "Y").groupBy("horizonId").count().where("count > 1").count() == 0
        assert actual.columns == [field.name for field in _hrm_schema()]
