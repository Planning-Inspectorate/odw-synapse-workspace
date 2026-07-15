import mock
import pyspark.sql.types as T
from pyspark.sql import Row
from odw.test.util.assertion import assert_dataframes_equal
from odw.core.etl.transformation.curated.pins_inspector_curated_process import (
    PinsInspectorCuratedProcess,
)
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


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


def _row(entra_id="entra-001", first_name="Alice", grade="G7"):
    return (
        entra_id,
        "00010001",
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
        Row(
            addressLine1="1 Main St",
            addressLine2=None,
            townCity="London",
            county="GL",
            postcode="EC1A 1AA",
        ),
        [Row(name="Planning", proficiency="Expert", validFrom="2020-01-01")],
        "2020-01-01T00:00:00.000Z",
    )


def _source_data(harmonised_df, curated_df=None):
    if curated_df is None:
        curated_df = harmonised_df.sparkSession.createDataFrame(
            [], harmonised_df.schema
        )
    return {
        "harmonised_inspector": harmonised_df,
        "curated_inspector": curated_df,
    }


def _process_under_test(spark):
    with mock.patch(
        "odw.core.etl.transformation.curated.curation_process.CurationProcess.__init__",
        return_value=None,
    ):
        inst = PinsInspectorCuratedProcess(spark)
    inst.spark = spark
    return inst


class TestPinsInspectorCuratedProcess(SparkTestCase):
    # ------------------------------------------------------------------
    # process – new records (no existing curated data)
    # ------------------------------------------------------------------

    def test__process__new_records_are_labelled_create(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame(
            [_row("entra-001"), _row("entra-002")], _hrm_schema()
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        labels = {
            row[PinsInspectorCuratedProcess._UPDATE_KEY_COL] for row in df.collect()
        }
        assert labels == {"create"}
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__process__passes_inspectors_through_to_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame(
            [_row("entra-001"), _row("entra-002")], _hrm_schema()
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        actual_df = df.select(
            "entraId", "sapId", "firstName", "lastName", "email", "grade", "validFrom"
        ).orderBy("entraId")

        expected_df = spark.createDataFrame(
            [
                (
                    "entra-001",
                    "00010001",
                    "Alice",
                    "Smith",
                    "alice@pins.gov.uk",
                    "G7",
                    "2020-01-01T00:00:00.000Z",
                ),
                (
                    "entra-002",
                    "00010001",
                    "Alice",
                    "Smith",
                    "alice@pins.gov.uk",
                    "G7",
                    "2020-01-01T00:00:00.000Z",
                ),
            ],
            actual_df.schema,
        )

        assert_dataframes_equal(actual_df, expected_df)
        assert result.metadata.insert_count == 2

    def test__process__output_has_exactly_the_expected_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame([_row()], _hrm_schema())

        inst = _process_under_test(spark)
        data_to_write, _ = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.columns == PinsInspectorCuratedProcess._OUTPUT_COLUMNS + [
            PinsInspectorCuratedProcess._UPDATE_KEY_COL
        ]

    def test__process__count_matches_insert_count(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame(
            [_row("entra-001"), _row("entra-002"), _row("entra-003")], _hrm_schema()
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 3
        assert result.metadata.insert_count == 3

    def test__process__write_config_uses_delta_merge(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame([_row()], _hrm_schema())

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        write_config = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]
        assert write_config["storage_kind"] == "ADLSG2-Delta"
        assert write_config["merge_keys"] == [PinsInspectorCuratedProcess._KEY_COL]
        assert (
            write_config["update_key_col"]
            == PinsInspectorCuratedProcess._UPDATE_KEY_COL
        )
        assert result.metadata.insert_count == 1

    def test__process__empty_input_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        harmonised_df = spark.createDataFrame([], _hrm_schema())

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 0
        assert df.columns == PinsInspectorCuratedProcess._OUTPUT_COLUMNS + [
            PinsInspectorCuratedProcess._UPDATE_KEY_COL
        ]
        assert result.metadata.insert_count == 0

    # ------------------------------------------------------------------
    # process – changed records (update)
    # ------------------------------------------------------------------

    def test__process__changed_record_is_labelled_update(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        curated_df = spark.createDataFrame(
            [_row("entra-001", first_name="OldName")], _hrm_schema()
        )
        harmonised_df = spark.createDataFrame(
            [_row("entra-001", first_name="NewName")], _hrm_schema()
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(harmonised_df, curated_df)
        )

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()
        assert len(rows) == 1
        assert rows[0][PinsInspectorCuratedProcess._UPDATE_KEY_COL] == "update"
        assert rows[0]["firstName"] == "NewName"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    # ------------------------------------------------------------------
    # process – unchanged records
    # ------------------------------------------------------------------

    def test__process__unchanged_record_is_excluded_from_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_row = _row("entra-001")
        curated_df = spark.createDataFrame([existing_row], _hrm_schema())
        harmonised_df = spark.createDataFrame([existing_row], _hrm_schema())

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(
            source_data=_source_data(harmonised_df, curated_df)
        )

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    # ------------------------------------------------------------------
    # process – null key filtering
    # ------------------------------------------------------------------

    def test__process__null_entra_id_is_excluded(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        def _null_key_row():
            r = list(_row())
            r[0] = None
            return tuple(r)

        harmonised_df = spark.createDataFrame(
            [_row("entra-001"), _null_key_row()], _hrm_schema()
        )

        inst = _process_under_test(spark)
        data_to_write, result = inst.process(source_data=_source_data(harmonised_df))

        df = data_to_write[PinsInspectorCuratedProcess.OUTPUT_TABLE]["data"]
        assert df.count() == 1
        assert result.metadata.insert_count == 1
