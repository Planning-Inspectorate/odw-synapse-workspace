from odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process import HorizonPinsInspectorHarmonisationProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


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


def _run(spark, horizon_rows):
    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.transformation.harmonised.horizon_pins_inspector_harmonisation_process.LoggingUtil"),
    ):
        inst = HorizonPinsInspectorHarmonisationProcess(spark)
        horizon_df = spark.createDataFrame(horizon_rows, schema=_horizon_schema())
        return inst.process(source_data={"horizon_data": horizon_df})


class TestHorizonPinsInspectorHarmonisationProcess(SparkTestCase):
    def test__horizon_pins_inspector__process__single_record_is_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, result = _run(
            spark,
            [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = scd2.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert result.metadata.insert_count == 1

    def test__horizon_pins_inspector__process__filters_null_horizon_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, result = _run(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                (None, "Bob", "Jones", "", "2024-01-01 10:00:00"),
            ],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        horizon_ids = [r["horizonId"] for r in scd2.collect()]

        assert "HZN-001" in horizon_ids
        assert None not in horizon_ids

    def test__horizon_pins_inspector__process__state_change_creates_two_versions(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, result = _run(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),  # lastName changed
            ],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = sorted(scd2.collect(), key=lambda r: str(r["IngestionDate"]))

        assert len(rows) == 2
        # First version should be expired
        assert rows[0]["IsActive"] == "N"
        assert rows[0]["ValidTo"] is not None
        # Second version should be active
        assert rows[1]["IsActive"] == "Y"
        assert rows[1]["ValidTo"] is None
        assert result.metadata.insert_count == 2

    def test__horizon_pins_inspector__process__identical_records_deduplicated_to_one(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, result = _run(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),  # exact duplicate
            ],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = scd2.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__horizon_pins_inspector__process__plumbing_fields_set_correctly(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, _ = _run(
            spark,
            [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = scd2.collect()

        assert rows[0]["Migrated"] == "0"
        assert rows[0]["ODTSourceSystem"] == "HORIZON"
        assert rows[0]["SourceSystemID"] == "Inspectors"

    def test__horizon_pins_inspector__process__stage_table_included_in_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, _ = _run(
            spark,
            [("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00")],
        )

        assert HorizonPinsInspectorHarmonisationProcess.STAGE_TABLE in data_to_write
        assert HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE in data_to_write

    def test__horizon_pins_inspector__process__two_inspectors_independent_scd2(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_to_write, result = _run(
            spark,
            [
                ("HZN-001", "Alice", "Smith", "", "2024-01-01 10:00:00"),
                ("HZN-001", "Alice", "Brown", "", "2024-06-01 10:00:00"),
                ("HZN-002", "Bob", "Jones", "", "2024-03-01 10:00:00"),
            ],
        )

        scd2 = data_to_write[HorizonPinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = scd2.collect()

        hzn1_rows = [r for r in rows if r["horizonId"] == "HZN-001"]
        hzn2_rows = [r for r in rows if r["horizonId"] == "HZN-002"]

        assert len(hzn1_rows) == 2
        assert len(hzn2_rows) == 1
        assert hzn2_rows[0]["IsActive"] == "Y"
        assert result.metadata.insert_count == 3
