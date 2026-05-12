import mock
import pyspark.sql.types as T
from pyspark.sql import Row
from odw.test.util.assertion import assert_dataframes_equal
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.pins_inspector_curated_process import PinsInspectorCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


def _hrm_schema():
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
            T.StructField(
                "specialisms",
                T.ArrayType(
                    T.StructType(
                        [
                            T.StructField("name", T.StringType(), True),
                            T.StructField("proficiency", T.StringType(), True),
                            T.StructField("validFrom", T.StringType(), True),
                        ]
                    )
                ),
                True,
            ),
            T.StructField("validFrom", T.StringType(), True),
        ]
    )


def _row(entra_id="entra-001", sap_id="00010001"):
    return (
        entra_id,
        sap_id,
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
        [Row(name="Planning", proficiency="Expert", validFrom="2020-01-01")],
        "2020-01-01T00:00:00.000Z",
    )


def _source_data(harmonised_data):
    return {"harmonised_inspector": harmonised_data}


class TestPinsInspectorCuratedProcess(ETLTestCase):
    def test__pins_inspector_curated_process__run__writes_active_inspectors_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = spark.createDataFrame([_row("entra-001"), _row("entra-002")], _hrm_schema())
        source_data = _source_data(harmonised_df)

        inst = PinsInspectorCuratedProcess(spark)

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
        } == {"insert_count": 2, "update_count": 0, "delete_count": 0}

        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert df.columns == PinsInspectorCuratedProcess._OUTPUT_COLUMNS

        actual_df = df.select("entraId", "sapId", "firstName", "lastName", "email", "grade", "validFrom").orderBy("entraId")

        expected_df = spark.createDataFrame(
            [
                ("entra-001", "00010001", "Alice", "Smith", "alice@pins.gov.uk", "G7", "2020-01-01T00:00:00.000Z"),
                ("entra-002", "00010001", "Alice", "Smith", "alice@pins.gov.uk", "G7", "2020-01-01T00:00:00.000Z"),
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)

    def test__pins_inspector_curated_process__run__empty_harmonised_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(spark.createDataFrame([], _hrm_schema()))

        inst = PinsInspectorCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 0
        assert df.columns == PinsInspectorCuratedProcess._OUTPUT_COLUMNS
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "delta"
        assert {
            "insert_count": result.metadata.insert_count,
            "update_count": result.metadata.update_count,
            "delete_count": result.metadata.delete_count,
        } == {"insert_count": 0, "update_count": 0, "delete_count": 0}
