from datetime import datetime
import mock
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.test.util.assertion import assert_dataframes_equal
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process import HorizonPinsInspectorHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


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


def _source_data(spark, rows=None):
    return {"horizon_data": spark.createDataFrame(rows or [], _horizon_schema())}


class TestHorizonPinsInspectorHarmonisationProcess(ETLTestCase):
    def test__horizon_pins_inspector__run__builds_scd2_timeline_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),
                ("HZN-002", "Bob", "Jones", "", "2024-03-01 10:00:00"),
            ],
        )

        inst = HorizonPinsInspectorHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {"insert_count": 3, "update_count": 0, "delete_count": 0}

        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert df.columns == [field.name for field in _hrm_schema()]

        actual_df = df.select("horizonId", "lastName", "IngestionDate", "ValidTo", "IsActive", "ODTSourceSystem").orderBy(
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

    def test__horizon_pins_inspector__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(spark)

        inst = HorizonPinsInspectorHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"

    def test__horizon_pins_inspector__run__deduplication_null_filtering_and_scd2_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),  # exact duplicate
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),  # state change
                (None, "Bad", "Row", "", "2024-01-01 10:00:00"),  # null key — filtered
            ],
        )

        inst = HorizonPinsInspectorHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert result.metadata.insert_count == 2
        assert df.where(F.col("horizonId").isNull()).count() == 0
        assert df.where(F.col("IsActive") == "Y").groupBy("horizonId").count().where("count > 1").count() == 0
        assert df.columns == [field.name for field in _hrm_schema()]
