from odw.core.etl.transformation.curated.pins_inspector_curated_process import PinsInspectorCuratedProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import Row
import pyspark.sql.types as T
import mock


def _harmonised_schema():
    return T.StructType(
        [
            T.StructField("entraId", T.StringType(), True),
            T.StructField("sapId", T.StringType(), True),
            T.StructField("firstName", T.StringType(), True),
            T.StructField("lastName", T.StringType(), True),
            T.StructField("email", T.StringType(), True),
            T.StructField("grade", T.StringType(), True),
            T.StructField("fte", T.StringType(), True),
            T.StructField("unit", T.StringType(), True),
            T.StructField("service", T.StringType(), True),
            T.StructField("group", T.StringType(), True),
            T.StructField("inspectorManager", T.StringType(), True),
            T.StructField("title", T.StringType(), True),
            T.StructField(
                "address",
                T.StructType(
                    [
                        T.StructField("addressLine1", T.StringType(), True),
                        T.StructField("addressLine2", T.StringType(), True),
                        T.StructField("townCity", T.StringType(), True),
                        T.StructField("county", T.StringType(), True),
                        T.StructField("postcode", T.StringType(), True),
                    ]
                ),
                True,
            ),
            T.StructField("specialisms", T.ArrayType(T.StringType()), True),
            T.StructField("validFrom", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _row(entra_id="entra-001", is_active="Y"):
    return (
        entra_id,
        "00010001",
        "Alice",
        "Smith",
        "alice@pins.gov.uk",
        "G7",
        "1.0",
        "NE",
        "Planning",
        "Group A",
        "Manager X",
        "Inspector",
        Row(addressLine1="1 Main St", addressLine2=None, townCity="London", county="GL", postcode="EC1A 1AA"),
        ["Planning"],
        "2020-01-01T00:00:00.000Z",
        is_active,
    )


def _run(spark, harmonised_rows):
    with (
        mock.patch(
            "odw.core.etl.transformation.curated.pins_inspector_curated_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.transformation.curated.pins_inspector_curated_process.LoggingUtil"),
    ):
        inst = PinsInspectorCuratedProcess(spark)
        harmonised_df = spark.createDataFrame(harmonised_rows, schema=_harmonised_schema())
        return inst.process(source_data={"harmonised_inspector": harmonised_df})


class TestPinsInspectorCuratedProcess(SparkTestCase):
    def test__pins_inspector_curated_process__process__returns_correct_output_table(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        data_to_write, _ = _run(spark, [_row()])

        assert PinsInspectorCuratedProcess.OUTPUT_TABLE in data_to_write

    def test__pins_inspector_curated_process__process__includes_all_expected_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        data_to_write, _ = _run(spark, [_row()])

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        for col in PinsInspectorCuratedProcess._OUTPUT_COLUMNS:
            assert col in df.columns, f"Expected column '{col}' not in output"

    def test__pins_inspector_curated_process__process__count_matches_insert_count_in_result(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [_row("entra-001"), _row("entra-002"), _row("entra-003")]
        data_to_write, result = _run(spark, rows)

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 3
        assert result.metadata.insert_count == 3

    def test__pins_inspector_curated_process__process__write_mode_is_overwrite(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        data_to_write, _ = _run(spark, [_row()])

        metadata = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]
        assert metadata["write_mode"] == "overwrite"
        assert metadata["file_format"] == "delta"
