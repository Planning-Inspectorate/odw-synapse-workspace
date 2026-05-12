import hashlib
import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.listed_building_harmonisation_process import ListedBuildingHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _standardised_schema():
    return StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("name", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("listed-building-grade", StringType(), True),
        ]
    )


def _harmonised_schema():
    return StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("endDate", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entryDate", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("listedBuildingGrade", StringType(), True),
            StructField("name", StringType(), True),
            StructField("organisationEntity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("startDate", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("documentationUrl", StringType(), True),
            StructField("dateReceived", StringType(), True),
            StructField("rowID", StringType(), True),
            StructField("validTo", StringType(), True),
            StructField("isActive", StringType(), True),
        ]
    )


def _standardised_row(**overrides):
    row = {
        "dataset": "listed-building",
        "end-date": None,
        "entity": "1001",
        "entry-date": "2024-01-01",
        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
        "name": "Building One",
        "organisation-entity": "org-1",
        "point": "POINT(1 1)",
        "prefix": "listed-building",
        "reference": "LB-001",
        "start-date": "2020-01-01",
        "typology": "grade-ii",
        "documentation-url": "https://example.com/lb-001",
        "listed-building-grade": "II",
    }
    row.update(overrides)
    return row


def _harmonised_row(**overrides):
    row = {
        "dataset": "listed-building",
        "endDate": None,
        "entity": "1001",
        "entryDate": "2024-01-01",
        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
        "listedBuildingGrade": "II",
        "name": "Old Building One",
        "organisationEntity": "org-1",
        "point": "POINT(1 1)",
        "prefix": "listed-building",
        "reference": "LB-001",
        "startDate": "2020-01-01",
        "typology": "grade-ii",
        "documentationUrl": "https://example.com/lb-001",
        "dateReceived": "2025-01-01",
        "rowID": "old-row-id",
        "validTo": None,
        "isActive": "Y",
    }
    row.update(overrides)
    return row


def _expected_rowid_for_standardised_row(row):
    values = [
        row.get("entity"),
        row.get("dataset"),
        row.get("end-date"),
        row.get("entry-date"),
        row.get("geometry"),
        row.get("listed-building-grade"),
        row.get("name"),
        row.get("organisation-entity"),
        row.get("point"),
        row.get("prefix"),
        row.get("reference"),
        row.get("start-date"),
        row.get("typology"),
        row.get("documentation-url"),
    ]
    joined = "".join(value if value is not None else "." for value in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


def _source_data(spark, source_rows=None, target_df=None):
    sd = {
        "source_data": spark.createDataFrame(source_rows or [], _standardised_schema()),
        "target_exists": target_df is not None,
    }
    if target_df is not None:
        sd["target_data"] = target_df
    return sd


def _existing_harmonised_df(spark):
    return spark.createDataFrame(
        [_harmonised_row()],
        schema=_harmonised_schema(),
    )


class TestListedBuildingHarmonisationProcess(ETLTestCase):
    def test__listed_building_harmonisation_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_rows = [
            _standardised_row(entity="1001", name="Building One"),
            _standardised_row(entity="1002", reference="LB-002", name="Building Two"),
        ]
        source_data = _source_data(spark, source_rows)
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert df.columns == [field.name for field in _harmonised_schema()]
        assert df.where(F.col("dateReceived").isNull()).count() == 0

        actual_df = df.where(F.col("entity") == "1001").select(
            "dataset", "endDate", "entryDate", "listedBuildingGrade", "organisationEntity",
            "documentationUrl", "isActive", "validTo",
        )
        expected_df = spark.createDataFrame(
            [("listed-building", None, "2024-01-01", "II", "org-1", "https://example.com/lb-001", "Y", None)],
            actual_df.schema,
        )
        assert_dataframes_equal(actual_df, expected_df)

    def test__listed_building_harmonisation_process__run__deactivates_old_version_and_inserts_new_version_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_rows = [_standardised_row(entity="1001", reference="LB-001", name="Building One Updated")]
        source_data = _source_data(spark, source_rows, target_df=_existing_harmonised_df(spark))
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        old_row = df.where((F.col("entity") == "1001") & (F.col("isActive") == "N")).select("name", "validTo", "rowID").collect()[0]
        new_row = df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).select("name", "validTo", "rowID").collect()[0]

        assert old_row["name"] == "Old Building One"
        assert old_row["validTo"] is not None
        assert new_row["name"] == "Building One Updated"
        assert new_row["validTo"] is None
        assert old_row["rowID"] != new_row["rowID"]

        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "N")).count() == 1
        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_harmonisation_process__run__inserts_new_reference_and_keeps_existing_active_row(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_rows = [_standardised_row(entity="1002", reference="LB-002", name="Building Two")]
        source_data = _source_data(spark, source_rows, target_df=_existing_harmonised_df(spark))
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("reference") == "LB-001") & (F.col("isActive") == "Y")).count() == 1
        assert df.where((F.col("reference") == "LB-002") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__run__does_not_duplicate_identical_active_row(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_row = _standardised_row(name="Old Building One")
        expected_rowid = _expected_rowid_for_standardised_row(source_row)
        existing_df = spark.createDataFrame(
            [_harmonised_row(rowID=expected_rowid)],
            schema=_harmonised_schema(),
        )
        source_data = _source_data(spark, [source_row], target_df=existing_df)
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        entity_rows = df.where(F.col("entity") == "1001")

        assert entity_rows.count() == 1
        assert entity_rows.where(F.col("isActive") == "Y").count() == 1
        assert entity_rows.where(F.col("isActive") == "N").count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__run__empty_source_returns_empty_output_on_initial_load(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark)
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__run__preserves_duplicate_source_rows_on_initial_load_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_rows = [_standardised_row(), _standardised_row()]
        source_data = _source_data(spark, source_rows)
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("reference") == "LB-001").count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__run__same_reference_different_entity_follows_legacy_join_and_count_behaviour(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_rows = [_standardised_row(entity="9999", reference="LB-001", name="Different Entity Same Reference")]
        source_data = _source_data(spark, source_rows, target_df=_existing_harmonised_df(spark))
        inst = ListedBuildingHarmonisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        # Legacy Notebook behaviour: the harmonisation merge/deactivation logic is keyed on entity, not reference
        # So when entity 9999 reuses reference LB-001 existing entity 1001 remains untouched
        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).count() == 1
        assert df.where((F.col("entity") == "9999") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1
