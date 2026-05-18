import uuid
import pyspark.sql.types as T
from pyspark.sql import Row
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.pins_inspector_curated_process import PinsInspectorCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


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
            T.StructField("isActive", T.StringType(), True),
        ]
    )


def _curated_schema():
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


def _hrm_row(entra_id="entra-001", sap_id="00010001", first_name="Alice", grade="G7", is_active="Y"):
    return (
        entra_id,
        sap_id,
        first_name,
        "Smith",
        "alice@pins.gov.uk",
        grade,
        "1.0",
        "NE",
        "Planning",
        "Group A",
        "Manager X",
        "Inspector",
        Row(addressLine1="1 Main St", addressLine2=None, townCity="London", county="GL", postcode="EC1A 1AA"),
        [Row(name="Planning", proficiency="Expert", validFrom="2020-01-01")],
        "2020-01-01T00:00:00.000Z",
        is_active,
    )


def _cur_row(entra_id="entra-001", sap_id="00010001", first_name="Alice", grade="G7"):
    return (
        entra_id,
        sap_id,
        first_name,
        "Smith",
        "alice@pins.gov.uk",
        grade,
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


class TestPinsInspectorCuratedProcess(ETLTestCase):
    def setup_method(self):
        self.test_suffix = uuid.uuid4().hex

    def _write_harmonised(self, spark, df):
        self.write_existing_table(
            spark,
            df,
            "pins_inspector",
            "odw_harmonised_db",
            "odw-harmonised",
            f"pins_inspector/{self.test_suffix}",
            "overwrite",
        )

    def _write_curated(self, spark, df):
        # blob_path matches the process config so SynapseDeltaIO can locate the delta table by path
        self.write_existing_table(
            spark,
            df,
            "pins_inspector",
            "odw_curated_db",
            "odw-curated",
            "pins_inspector",
            "overwrite",
        )

    def test__run__writes_active_inspectors_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_harmonised(spark, spark.createDataFrame([_hrm_row("entra-001"), _hrm_row("entra-002")], _harmonised_schema()))
        self._write_curated(spark, spark.createDataFrame([], _curated_schema()))

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE)

        assert actual.count() == 2
        assert set(actual.columns) == set(PinsInspectorCuratedProcess._OUTPUT_COLUMNS)

        actual_df = actual.select("entraId", "sapId", "firstName", "lastName", "email", "grade", "validFrom").orderBy("entraId")
        expected_df = spark.createDataFrame(
            [
                ("entra-001", "00010001", "Alice", "Smith", "alice@pins.gov.uk", "G7", "2020-01-01T00:00:00.000Z"),
                ("entra-002", "00010001", "Alice", "Smith", "alice@pins.gov.uk", "G7", "2020-01-01T00:00:00.000Z"),
            ],
            actual_df.schema,
        )
        assert_dataframes_equal(actual_df, expected_df)

    def test__run__empty_harmonised_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_harmonised(spark, spark.createDataFrame([], _harmonised_schema()))
        self._write_curated(spark, spark.createDataFrame([], _curated_schema()))

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE)
        assert actual.count() == 0
        assert set(actual.columns) == set(PinsInspectorCuratedProcess._OUTPUT_COLUMNS)

    def test__run__changed_record_updated_unchanged_record_preserved(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_harmonised(
            spark,
            spark.createDataFrame(
                [_hrm_row("entra-001", first_name="NewName"), _hrm_row("entra-002")],
                _harmonised_schema(),
            ),
        )
        self._write_curated(
            spark,
            spark.createDataFrame(
                [_cur_row("entra-001", first_name="OldName"), _cur_row("entra-002")],
                _curated_schema(),
            ),
        )

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE).orderBy("entraId")
        rows = actual.collect()

        assert len(rows) == 2
        assert rows[0]["entraId"] == "entra-001"
        assert rows[0]["firstName"] == "NewName"
        assert rows[1]["entraId"] == "entra-002"
        assert rows[1]["firstName"] == "Alice"
