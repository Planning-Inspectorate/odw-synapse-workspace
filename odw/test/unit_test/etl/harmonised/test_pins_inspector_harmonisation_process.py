from odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process import PinsInspectorHarmonisationProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
from pyspark.sql import Row
import mock


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


def _source_data(spark, live_dim_rows, entraid_rows=None, specialisms_rows=None, address_rows=None, hist_hr_rows=None):
    return {
        "live_dim": spark.createDataFrame(live_dim_rows or [], schema=_live_dim_schema()),
        "entraid": spark.createDataFrame(entraid_rows or [], schema=_entraid_schema()),
        "specialisms": spark.createDataFrame(specialisms_rows or [], schema=_specialisms_schema()),
        "address": spark.createDataFrame(address_rows or [], schema=_address_schema()),
        "hist_hr": spark.createDataFrame(hist_hr_rows or [], schema=_hist_hr_schema()),
    }


def _inst(spark):
    with mock.patch(
        "odw.core.etl.transformation.harmonised.harmonsation_process.HarmonisationProcess.__init__",
        return_value=None,
    ):
        inst = PinsInspectorHarmonisationProcess(spark)
    inst.spark = spark
    return inst


class TestDedupSpecialisms(SparkTestCase):
    def test__dedup_specialisms__latest_per_qualification_kept(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [
            ("00010001", "Planning", "Expert", "2023-01-01", 1),
            ("00010001", "Planning", "Beginner", "2022-01-01", 1),
            ("00010001", "Transport", "Intermediate", "2023-06-01", 1),
        ]
        df = spark.createDataFrame(rows, schema=_specialisms_schema())

        result = _inst(spark)._dedup_specialisms(df).collect()

        assert len(result) == 1
        spec_map = {s["name"]: s["proficiency"] for s in result[0]["specialisms"]}
        assert spec_map["Planning"] == "Expert"
        assert spec_map["Transport"] == "Intermediate"

    def test__dedup_specialisms__inactive_records_excluded(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [
            ("00010001", "Planning", "Expert", "2023-01-01", 1),
            ("00010001", "Heritage", "Expert", "2023-01-01", 0),
        ]
        df = spark.createDataFrame(rows, schema=_specialisms_schema())

        result = _inst(spark)._dedup_specialisms(df).collect()

        names = {s["name"] for s in result[0]["specialisms"]}
        assert names == {"Planning"}

    def test__dedup_specialisms__empty_input_returns_no_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df = spark.createDataFrame([], schema=_specialisms_schema())

        result = _inst(spark)._dedup_specialisms(df)

        assert result.count() == 0

    def test__dedup_specialisms__output_renamed_to_sapId(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [("00010001", "Planning", "Expert", "2023-01-01", 1)]
        df = spark.createDataFrame(rows, schema=_specialisms_schema())

        result = _inst(spark)._dedup_specialisms(df)

        assert "sapId" in result.columns
        assert "StaffNumber" not in result.columns


class TestPadEntraid(SparkTestCase):
    def test__pad_entraid__short_id_gets_00_prefix(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df = spark.createDataFrame([("entra-1", "123456", "Y")], schema=_entraid_schema())

        result = _inst(spark)._pad_entraid(df).collect()

        assert len(result) == 1
        assert result[0]["employeeId"] == "00123456"

    def test__pad_entraid__eight_char_id_unchanged(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df = spark.createDataFrame([("entra-1", "12345678", "Y")], schema=_entraid_schema())

        result = _inst(spark)._pad_entraid(df).collect()

        assert result[0]["employeeId"] == "12345678"

    def test__pad_entraid__inactive_records_excluded(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        df = spark.createDataFrame(
            [("entra-1", "00010001", "Y"), ("entra-2", "00010002", "N")],
            schema=_entraid_schema(),
        )

        result = _inst(spark)._pad_entraid(df).collect()

        assert len(result) == 1
        assert result[0]["id"] == "entra-1"


class TestDedupAddress(SparkTestCase):
    def test__dedup_address__latest_by_ingestion_date_kept(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [
            ("00010001", "2024-01-01", "1 New St", "Floor 2", "London", "GL", "EC1A 1BB"),
            ("00010001", "2023-01-01", "1 Old St", "Floor 1", "London", "GL", "EC1A 1AA"),
        ]
        df = spark.createDataFrame(rows, schema=_address_schema())

        result = _inst(spark)._dedup_address(df).collect()

        assert len(result) == 1
        assert result[0]["StreetandHouseNumber"] == "1 New St"

    def test__dedup_address__2nd_address_line_renamed(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [("00010001", "2024-01-01", "1 Main St", "Suite 5", "London", "GL", "EC1A 1AA")]
        df = spark.createDataFrame(rows, schema=_address_schema())

        result = _inst(spark)._dedup_address(df)

        assert "addressLine2" in result.columns
        assert "2ndAddressLine" not in result.columns

    def test__dedup_address__one_row_per_staff_number(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [
            ("00010001", "2024-01-01", "A", None, "X", "Y", "AA1"),
            ("00010001", "2023-06-01", "B", None, "X", "Y", "AA2"),
            ("00010002", "2024-01-01", "C", None, "X", "Y", "BB1"),
        ]
        df = spark.createDataFrame(rows, schema=_address_schema())

        result = _inst(spark)._dedup_address(df)

        assert result.count() == 2


class TestLatestHr(SparkTestCase):
    def test__latest_hr__most_recent_per_person_kept(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [
            ("00010001", "Inspector", "1.0", "NE", "Planning", "Group A", "Manager X", "SAP", "2024-06-01"),
            ("00010001", "Trainee", "0.5", "SW", "Transport", "Group B", "Manager Y", "SAP", "2023-01-01"),
        ]
        df = spark.createDataFrame(rows, schema=_hist_hr_schema())

        result = _inst(spark)._latest_hr(df).collect()

        assert len(result) == 1
        assert result[0]["Position1"] == "Inspector"

    def test__latest_hr__selects_expected_columns_only(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        rows = [("00010001", "Inspector", "1.0", "NE", "Planning", "Group A", "Manager X", "SAP", "2024-06-01")]
        df = spark.createDataFrame(rows, schema=_hist_hr_schema())

        result = _inst(spark)._latest_hr(df)

        assert set(result.columns) == {"PersNo", "Position1", "FTE", "PersonnelArea", "PersonnelSubArea", "OrganizationalUnit", "NameofManagerOM", "SourceSystemID"}
        assert "ingestionDate" not in result.columns


class TestJoinInspectors(SparkTestCase):
    def _make_frames(self, spark):
        live_dim = spark.createDataFrame(
            [("00010001", "alice@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
            schema=_live_dim_schema(),
        )
        spec_dedup = spark.createDataFrame(
            [Row(sapId="00010001", specialisms=[Row(name="Planning", proficiency="Expert", validFrom="2020-01-01")])],
            schema=T.StructType([
                T.StructField("sapId", T.StringType(), True),
                T.StructField("specialisms", T.ArrayType(T.StructType([
                    T.StructField("name", T.StringType(), True),
                    T.StructField("proficiency", T.StringType(), True),
                    T.StructField("validFrom", T.StringType(), True),
                ])), True),
            ]),
        )
        entraid = spark.createDataFrame(
            [("entra-001", "00010001", "Y")],
            schema=_entraid_schema(),
        )
        address = spark.createDataFrame(
            [("00010001", "2024-01-01", "1 Main St", None, "London", "GL", "EC1A 1AA")],
            schema=_address_schema(),
        ).withColumnRenamed("2ndAddressLine", "addressLine2")
        hr = spark.createDataFrame(
            [("00010001", "Inspector", "1.0", "NE", "Planning", "Group A", "Manager X", "SAP")],
            schema=T.StructType([
                T.StructField("PersNo", T.StringType(), True),
                T.StructField("Position1", T.StringType(), True),
                T.StructField("FTE", T.StringType(), True),
                T.StructField("PersonnelArea", T.StringType(), True),
                T.StructField("PersonnelSubArea", T.StringType(), True),
                T.StructField("OrganizationalUnit", T.StringType(), True),
                T.StructField("NameofManagerOM", T.StringType(), True),
                T.StructField("SourceSystemID", T.StringType(), True),
            ]),
        )
        return live_dim, spec_dedup, entraid, hr, address

    def test__join_inspectors__non_active_excluded(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        live_dim = spark.createDataFrame(
            [
                ("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE"),
                ("00010002", "b@pins.gov.uk", "Bob", "Jones", "2021-01-01", "G6", "N", "NON-ACTIVE"),
            ],
            schema=_live_dim_schema(),
        )
        empty_spec = spark.createDataFrame([], T.StructType([
            T.StructField("sapId", T.StringType(), True),
            T.StructField("specialisms", T.ArrayType(T.StructType([
                T.StructField("name", T.StringType(), True),
                T.StructField("proficiency", T.StringType(), True),
                T.StructField("validFrom", T.StringType(), True),
            ])), True),
        ]))
        empty_eid = spark.createDataFrame([], _entraid_schema())
        empty_addr = spark.createDataFrame([], _address_schema()).withColumnRenamed("2ndAddressLine", "addressLine2")
        empty_hr = spark.createDataFrame([], T.StructType([
            T.StructField("PersNo", T.StringType(), True),
            T.StructField("Position1", T.StringType(), True),
            T.StructField("FTE", T.StringType(), True),
            T.StructField("PersonnelArea", T.StringType(), True),
            T.StructField("PersonnelSubArea", T.StringType(), True),
            T.StructField("OrganizationalUnit", T.StringType(), True),
            T.StructField("NameofManagerOM", T.StringType(), True),
            T.StructField("SourceSystemID", T.StringType(), True),
        ]))

        result = _inst(spark)._join_inspectors(live_dim, empty_spec, empty_eid, empty_addr, empty_hr)

        sap_ids = {r["sapId"] for r in result.collect()}
        assert "00010001" in sap_ids
        assert "00010002" not in sap_ids

    def test__join_inspectors__output_columns_present(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        live_dim, spec_dedup, entraid_padded, hr, addr = self._make_frames(spark)

        result = _inst(spark)._join_inspectors(live_dim, spec_dedup, entraid_padded, addr, hr)

        expected_cols = {"entraId", "sapId", "email", "firstName", "lastName", "validFrom", "grade", "isActive", "specialisms", "address", "fte", "unit", "service", "group", "inspectorManager", "title", "sourceSystem"}
        assert set(result.columns) == expected_cols

    def test__join_inspectors__specialisms_joined_correctly(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        live_dim, spec_dedup, entraid_padded, hr, addr = self._make_frames(spark)

        result = _inst(spark)._join_inspectors(live_dim, spec_dedup, entraid_padded, addr, hr).collect()

        assert len(result) == 1
        assert result[0]["specialisms"][0]["name"] == "Planning"


class TestPinsInspectorHarmonisationProcess(SparkTestCase):
    def _run(self, spark, source_data):
        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch(
                "odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process.Util.is_non_production_environment",
                return_value=False,
            ),
            mock.patch("odw.core.etl.transformation.harmonised.pins_inspector_harmonisation_process.LoggingUtil"),
        ):
            inst = PinsInspectorHarmonisationProcess(spark)
            return inst.process(source_data=source_data)

    def test__pins_inspector_harmonisation_process__process__excludes_non_active_inspectors(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[
                ("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE"),
                ("00010002", "b@pins.gov.uk", "Bob", "Jones", "2021-01-01", "G6", "N", "NON-ACTIVE"),
            ],
        )
        data_to_write, result = self._run(spark, source)

        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        sap_ids = [r["sapId"] for r in df.collect()]

        assert "00010001" in sap_ids
        assert "00010002" not in sap_ids
        assert result.metadata.insert_count == 1

    def test__pins_inspector_harmonisation_process__process__pads_short_entraid_employee_ids(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        # SAP number in live_dim is "0012345" (7 chars).
        # EntraID "entra-match" stores the old short form "12345" (5 chars, < 8).
        # Padding gives "00" + "12345" = "0012345" which joins to the live_dim SAP number.
        # EntraID "entra-no-match" stores "0012345" (7 chars); padded becomes "000012345" — no match.
        source = _source_data(
            spark,
            live_dim_rows=[("0012345", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
            entraid_rows=[
                ("entra-match", "12345", "Y"),  # short → padded "0012345" → matches live_dim
                ("entra-no-match", "0012345", "Y"),  # 7 chars → padded "000012345" → no match
            ],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["entraId"] == "entra-match"

    def test__pins_inspector_harmonisation_process__process__deduplicates_specialisms_per_qualification(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
            specialisms_rows=[
                # Same qualification, two dates — only latest should survive
                ("00010001", "Planning", "Expert", "2023-01-01", 1),
                ("00010001", "Planning", "Beginner", "2022-01-01", 1),
                # Different qualification
                ("00010001", "Transport", "Intermediate", "2023-06-01", 1),
                # Inactive specialism — excluded
                ("00010001", "Heritage", "Expert", "2023-01-01", 0),
            ],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        specialisms = rows[0]["specialisms"]
        names = {s["name"] for s in specialisms}

        assert "Planning" in names
        assert "Transport" in names
        assert "Heritage" not in names

        planning = next(s for s in specialisms if s["name"] == "Planning")
        assert planning["proficiency"] == "Expert"  # latest

    def test__pins_inspector_harmonisation_process__process__deduplicates_address_keeps_latest(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
            address_rows=[
                ("00010001", "2024-01-01", "1 New St", "Floor 2", "London", "Greater London", "EC1A 1BB"),
                ("00010001", "2023-01-01", "1 Old St", "Floor 1", "London", "Greater London", "EC1A 1AA"),
            ],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["address"]["addressLine1"] == "1 New St"
        assert rows[0]["address"]["postcode"] == "EC1A 1BB"

    def test__pins_inspector_harmonisation_process__process__hr_keeps_latest_record_per_person(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
            hist_hr_rows=[
                ("00010001", "Inspector", "1.0", "NE", "Planning", "Group A", "Manager X", "SAP", "2024-06-01"),
                ("00010001", "Senior Inspector", "0.8", "SW", "Transport", "Group B", "Manager Y", "SAP", "2023-01-01"),
            ],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["title"] == "Inspector"
        assert rows[0]["unit"] == "NE"

    def test__pins_inspector_harmonisation_process__process__appends_iso_suffix_to_valid_from(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-03-15", "G7", "Y", "ACTIVE")],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["validFrom"] == "2020-03-15T00:00:00.000Z"

    def test__pins_inspector_harmonisation_process__process__null_valid_from_stays_null(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", None, "G7", "Y", "ACTIVE")],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["validFrom"] is None

    def test__pins_inspector_harmonisation_process__process__empty_specialisms_defaults_to_empty_array(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source = _source_data(
            spark,
            live_dim_rows=[("00010001", "a@pins.gov.uk", "Alice", "Smith", "2020-01-01", "G7", "Y", "ACTIVE")],
        )
        data_to_write, _ = self._run(spark, source)
        df = data_to_write[PinsInspectorHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["specialisms"] == []
