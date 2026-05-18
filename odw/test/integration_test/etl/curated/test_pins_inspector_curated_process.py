import uuid
import mock
import pyspark.sql.types as T
from pyspark.sql import functions as F
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.pins_inspector_curated_process import PinsInspectorCuratedProcess
from odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process import PinsInspectorHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


def _entraid_schema():
    return T.StructType(
        [
            T.StructField("id", T.StringType(), True),
            T.StructField("employeeId", T.StringType(), True),
            T.StructField("isActive", T.StringType(), True),
        ]
    )


def _specialisms_schema():
    return T.StructType(
        [
            T.StructField("StaffNumber", T.StringType(), True),
            T.StructField("QualificationName", T.StringType(), True),
            T.StructField("Proficien", T.StringType(), True),
            T.StructField("ValidFrom", T.StringType(), True),
            T.StructField("Current", T.IntegerType(), True),
        ]
    )


def _address_schema():
    return T.StructType(
        [
            T.StructField("StaffNumber", T.StringType(), True),
            T.StructField("IngestionDate", T.StringType(), True),
            T.StructField("StreetandHouseNumber", T.StringType(), True),
            T.StructField("2ndAddressLine", T.StringType(), True),
            T.StructField("City", T.StringType(), True),
            T.StructField("District", T.StringType(), True),
            T.StructField("PostalCode", T.StringType(), True),
        ]
    )


def _live_dim_schema():
    return T.StructType(
        [
            T.StructField("pins_staff_number", T.StringType(), True),
            T.StructField("pins_email_address", T.StringType(), True),
            T.StructField("given_names", T.StringType(), True),
            T.StructField("family_name", T.StringType(), True),
            T.StructField("date_in", T.StringType(), True),
            T.StructField("grade", T.StringType(), True),
            T.StructField("isActive", T.StringType(), True),
            T.StructField("active_status", T.StringType(), True),
        ]
    )


def _hist_hr_schema():
    return T.StructType(
        [
            T.StructField("PersNo", T.StringType(), True),
            T.StructField("Position1", T.StringType(), True),
            T.StructField("FTE", T.StringType(), True),
            T.StructField("PersonnelArea", T.StringType(), True),
            T.StructField("PersonnelSubArea", T.StringType(), True),
            T.StructField("OrganizationalUnit", T.StringType(), True),
            T.StructField("NameofManagerOM", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
            T.StructField("ingestionDate", T.StringType(), True),
        ]
    )


def _live_dim_row(sap_id, first_name="Alice", last_name="Smith", email=None, grade="G7", is_active="Y", active_status="ACTIVE"):
    return (sap_id, email or f"{first_name.lower()}@pins.gov.uk", first_name, last_name, "2020-01-01", grade, is_active, active_status)


def _entraid_row(entra_id, sap_id):
    return (entra_id, sap_id, "Y")


def _source_data(spark, live_dim_rows=None, entraid_rows=None, specialisms_rows=None, address_rows=None, hist_hr_rows=None):
    return {
        "live_dim": spark.createDataFrame(live_dim_rows or [], _live_dim_schema()),
        "entraid": spark.createDataFrame(entraid_rows or [], _entraid_schema()),
        "specialisms": spark.createDataFrame(specialisms_rows or [], _specialisms_schema()),
        "address": spark.createDataFrame(address_rows or [], _address_schema()),
        "hist_hr": spark.createDataFrame(hist_hr_rows or [], _hist_hr_schema()),
    }


class TestPinsInspectorCuratedProcess(ETLTestCase):
    def setup_method(self):
        self.test_suffix = uuid.uuid4().hex

    def _harmonise(self, spark, live_dim_rows=None, entraid_rows=None, specialisms_rows=None, address_rows=None, hist_hr_rows=None):
        """Run the harmonisation process in-memory and return its output DataFrame."""
        source = _source_data(spark, live_dim_rows, entraid_rows, specialisms_rows, address_rows, hist_hr_rows)
        with mock.patch(
            "odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process.Util.is_non_production_environment",
            return_value=False,
        ):
            inst = PinsInspectorHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source)
        return data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]

    def _write_harmonised(self, spark, harmonised_df):
        self.write_existing_table(
            spark,
            harmonised_df,
            "pins_inspector",
            "odw_harmonised_db",
            "odw-harmonised",
            f"pins_inspector/{self.test_suffix}",
            "overwrite",
        )

    def _write_curated(self, spark, df):
        # blob_path matches the process config so SynapseDeltaIO can locate the delta table by path
        self.write_existing_table(spark, df, "pins_inspector", "odw_curated_db", "odw-curated", "pins_inspector", "overwrite")

    def _empty_curated(self, spark, harmonised_df):
        empty = harmonised_df.filter(F.lit(False)).select(*PinsInspectorCuratedProcess._OUTPUT_COLUMNS)
        self._write_curated(spark, empty)

    def test__run__writes_active_inspectors_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = self._harmonise(
            spark,
            live_dim_rows=[_live_dim_row("00010001", first_name="Alice"), _live_dim_row("00010002", first_name="Bob")],
            entraid_rows=[_entraid_row("entra-001", "00010001"), _entraid_row("entra-002", "00010002")],
        )
        self._write_harmonised(spark, harmonised_df)
        self._empty_curated(spark, harmonised_df)

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE).orderBy("entraId")

        expected = harmonised_df.filter(F.col("isActive") == "Y").select(*PinsInspectorCuratedProcess._OUTPUT_COLUMNS).orderBy("entraId")

        assert actual.count() == 2

        # Compare scalar columns; nested types (address, specialisms) differ in nullability
        # after a Delta roundtrip and are covered by the harmonisation unit tests
        scalar_cols = [
            "entraId",
            "sapId",
            "firstName",
            "lastName",
            "email",
            "grade",
            "fte",
            "unit",
            "service",
            "group",
            "inspectorManager",
            "title",
            "validFrom",
        ]
        assert_dataframes_equal(actual.select(scalar_cols), expected.select(scalar_cols))

    def test__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_df = self._harmonise(spark)
        self._write_harmonised(spark, harmonised_df)
        self._empty_curated(spark, harmonised_df)

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE)
        assert actual.count() == 0

    def test__run__changed_record_updated_unchanged_record_preserved(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        # Harmonise the initial state and seed the curated table from it
        initial_harmonised = self._harmonise(
            spark,
            live_dim_rows=[_live_dim_row("00010001", first_name="OldName"), _live_dim_row("00010002", first_name="Bob")],
            entraid_rows=[_entraid_row("entra-001", "00010001"), _entraid_row("entra-002", "00010002")],
        )
        initial_curated = initial_harmonised.filter(F.col("isActive") == "Y").select(*PinsInspectorCuratedProcess._OUTPUT_COLUMNS)
        self._write_curated(spark, initial_curated)

        # Harmonise updated source data (entra-001 first name changed)
        updated_harmonised = self._harmonise(
            spark,
            live_dim_rows=[_live_dim_row("00010001", first_name="NewName"), _live_dim_row("00010002", first_name="Bob")],
            entraid_rows=[_entraid_row("entra-001", "00010001"), _entraid_row("entra-002", "00010002")],
        )
        self._write_harmonised(spark, updated_harmonised)

        result = PinsInspectorCuratedProcess(spark).run()

        assert_etl_result_successful(result)
        actual = spark.table(PinsInspectorCuratedProcess.OUTPUT_TABLE).orderBy("entraId")
        rows = actual.collect()

        assert len(rows) == 2
        assert rows[0]["entraId"] == "entra-001"
        assert rows[0]["firstName"] == "NewName"
        assert rows[1]["entraId"] == "entra-002"
        assert rows[1]["firstName"] == "Bob"
