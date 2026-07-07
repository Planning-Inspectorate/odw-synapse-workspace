from datetime import datetime
import mock
import pytest
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.test.util.assertion import assert_dataframes_equal
from odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process import (
    HorizonPinsInspectorHarmonisationProcess,
)
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


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


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.harmonised.harmonisation_process.HarmonisationProcess.__init__",
        return_value=None,
    ):
        inst = HorizonPinsInspectorHarmonisationProcess(spark)
    inst.spark = spark
    return inst


class TestHorizonPinsInspectorHarmonisationProcess(SparkTestCase):
    @pytest.fixture(autouse=True)
    def _patch_util(self):
        with mock.patch(
            "odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ):
            yield

    def test__horizon_pins_inspector__process__single_record_is_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(
                spark, [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")]
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]
        actual_df = df.select(
            "horizonId",
            "firstName",
            "lastName",
            "IsActive",
            "ValidTo",
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
        )

        expected_df = spark.createDataFrame(
            [("HZN-001", "Alice", "Smith", "Y", None, "0", "HORIZON", "Inspectors")],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 1

    def test__horizon_pins_inspector__process__filters_null_horizon_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(
                spark,
                [
                    ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                    (None, "Bob", "Jones", "", "2024-01-01 10:00:00"),
                ],
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]
        horizon_ids = [r["horizonId"] for r in df.collect()]

        assert "HZN-001" in horizon_ids
        assert None not in horizon_ids
        assert result.metadata.insert_count == 1

    def test__horizon_pins_inspector__process__state_change_creates_two_versions(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(
                spark,
                [
                    ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                    ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),
                ],
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]
        actual_df = df.select(
            "horizonId", "lastName", "IngestionDate", "ValidTo", "IsActive"
        ).orderBy("IngestionDate")

        expected_df = spark.createDataFrame(
            [
                (
                    "HZN-001",
                    "Smith",
                    datetime(2024, 1, 1, 10),
                    datetime(2024, 6, 1, 10),
                    "N",
                ),
                ("HZN-001", "Brown", datetime(2024, 6, 1, 10), None, "Y"),
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__horizon_pins_inspector__process__identical_records_deduplicated_to_one(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(
                spark,
                [
                    ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                    ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ],
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]
        actual_df = df.select("horizonId", "IsActive", "ValidTo")

        expected_df = spark.createDataFrame(
            [("HZN-001", "Y", None)],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 1

    def test__horizon_pins_inspector__process__plumbing_fields_set_correctly(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(
            source_data=_source_data(
                spark, [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")]
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]
        actual_df = df.select("Migrated", "ODTSourceSystem", "SourceSystemID")

        expected_df = spark.createDataFrame(
            [("0", "HORIZON", "Inspectors")],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

    def test__horizon_pins_inspector__process__stage_table_included_in_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(
            source_data=_source_data(
                spark, [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")]
            )
        )

        assert HorizonPinsInspectorHarmonisationProcess.STAGE_TABLE in data_to_write
        assert HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE in data_to_write

    def test__horizon_pins_inspector__process__two_inspectors_have_independent_scd2_timelines(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(
                spark,
                [
                    ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                    ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),
                    ("HZN-002", "Bob", "Jones", "", "2024-03-01 10:00:00"),
                ],
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]

        assert df.filter(F.col("horizonId") == "HZN-001").count() == 2
        assert df.filter(F.col("horizonId") == "HZN-002").count() == 1
        assert (
            df.filter(
                (F.col("horizonId") == "HZN-001") & (F.col("IsActive") == "Y")
            ).count()
            == 1
        )
        assert (
            df.filter(
                (F.col("horizonId") == "HZN-002") & (F.col("IsActive") == "Y")
            ).count()
            == 1
        )
        assert result.metadata.insert_count == 3

    def test__horizon_pins_inspector__process__keeps_expected_output_column_order(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(
            source_data=_source_data(
                spark, [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")]
            )
        )

        df = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE][
            "data"
        ]

        assert df.columns == [field.name for field in _hrm_schema()]
