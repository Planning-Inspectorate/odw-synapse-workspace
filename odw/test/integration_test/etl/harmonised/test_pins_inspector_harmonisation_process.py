import uuid
import mock
import pyspark.sql.types as T
from odw.test.util.assertion import assert_etl_result_successful
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
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


def _live_dim_row(sap_id, first_name="Alice", last_name="Smith", email=None, grade="G7", active_status="ACTIVE"):
    return (sap_id, email or f"{first_name.lower()}@pins.gov.uk", first_name, last_name, "2020-01-01", grade, "Y", active_status)


class TestPinsInspectorHarmonisationProcess(ETLTestCase):
    def setup_method(self):
        self.test_suffix = uuid.uuid4().hex

    def _write_entraid(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _entraid_schema()),
            "entraid",
            "odw_harmonised_db",
            "odw-harmonised",
            f"entraid/{self.test_suffix}",
            "overwrite",
        )

    def _write_live_dim(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _live_dim_schema()),
            "live_dim_inspector",
            "odw_harmonised_db",
            "odw-harmonised",
            f"live_dim_inspector/{self.test_suffix}",
            "overwrite",
        )

    def _write_specialisms(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _specialisms_schema()),
            "sap_hr_inspector_specialisms",
            "odw_harmonised_db",
            "odw-harmonised",
            f"sap_hr_inspector_specialisms/{self.test_suffix}",
            "overwrite",
        )

    def _write_address(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _address_schema()),
            "sap_hr_inspector_address",
            "odw_harmonised_db",
            "odw-harmonised",
            f"sap_hr_inspector_address/{self.test_suffix}",
            "overwrite",
        )

    def _write_hist_hr(self, spark, rows=None):
        self.write_existing_table(
            spark,
            spark.createDataFrame(rows or [], _hist_hr_schema()),
            "hist_sap_hr",
            "odw_harmonised_db",
            "odw-harmonised",
            f"hist_sap_hr/{self.test_suffix}",
            "overwrite",
        )

    def _run(self, spark):
        source_data = {
            "entraid": spark.table(PinsInspectorHarmonisationProcess.ENTRAID_TABLE),
            "specialisms": spark.table(PinsInspectorHarmonisationProcess.INSPECTOR_SPECIALISMS_TABLE),
            "address": spark.table(PinsInspectorHarmonisationProcess.INSPECTOR_ADDRESS_TABLE),
            "live_dim": spark.table(PinsInspectorHarmonisationProcess.LIVE_DIM_TABLE),
            "hist_hr": spark.table(PinsInspectorHarmonisationProcess.HIST_SAP_HR_TABLE),
        }
        with mock.patch(
            "odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process.Util.is_non_production_environment",
            return_value=False,
        ):
            data_to_write, result = PinsInspectorHarmonisationProcess(spark).process(source_data=source_data)
        return data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"], result

    def test__run__active_inspectors_written_end_to_end(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_live_dim(spark, [_live_dim_row("00010001", first_name="Alice"), _live_dim_row("00010002", first_name="Bob")])
        self._write_entraid(spark, [("entra-001", "00010001", "Y"), ("entra-002", "00010002", "Y")])
        self._write_specialisms(spark)
        self._write_address(spark)
        self._write_hist_hr(spark)

        actual, result = self._run(spark)

        assert_etl_result_successful(result)
        assert result.metadata.insert_count == 2, f"Expected 2 rows from process, got {result.metadata.insert_count}"
        rows = actual.orderBy("sapId").collect()

        assert len(rows) == 2
        assert rows[0]["sapId"] == "00010001"
        assert rows[0]["entraId"] == "entra-001"
        assert rows[0]["firstName"] == "Alice"
        assert rows[1]["sapId"] == "00010002"
        assert rows[1]["entraId"] == "entra-002"
        assert rows[1]["firstName"] == "Bob"

    def test__run__non_active_inspectors_excluded(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_live_dim(
            spark,
            [
                _live_dim_row("00010001", first_name="Active", active_status="ACTIVE"),
                _live_dim_row("00010002", first_name="NonActive", active_status="NON-ACTIVE"),
            ],
        )
        self._write_entraid(spark, [("entra-001", "00010001", "Y"), ("entra-002", "00010002", "Y")])
        self._write_specialisms(spark)
        self._write_address(spark)
        self._write_hist_hr(spark)

        actual, result = self._run(spark)

        assert_etl_result_successful(result)
        assert result.metadata.insert_count == 1
        rows = actual.collect()
        assert len(rows) == 1
        assert rows[0]["firstName"] == "Active"

    def test__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_live_dim(spark)
        self._write_entraid(spark)
        self._write_specialisms(spark)
        self._write_address(spark)
        self._write_hist_hr(spark)

        actual, result = self._run(spark)

        assert_etl_result_successful(result)
        assert actual.count() == 0
        assert result.metadata.insert_count == 0

    def test__run__valid_from_has_iso8601_suffix(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        self._write_live_dim(spark, [_live_dim_row("00010001")])
        self._write_entraid(spark, [("entra-001", "00010001", "Y")])
        self._write_specialisms(spark)
        self._write_address(spark)
        self._write_hist_hr(spark)

        actual, _ = self._run(spark)

        assert actual.collect()[0]["validFrom"] == "2020-01-01T00:00:00.000Z"
