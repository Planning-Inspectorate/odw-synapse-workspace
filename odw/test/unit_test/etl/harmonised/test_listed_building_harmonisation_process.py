import hashlib
import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.listed_building_harmonisation_process import ListedBuildingHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

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


class TestListedBuildingHarmonisationProcess(SparkTestCase):
    def test__listed_building_harmonisation_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = ListedBuildingHarmonisationProcess(spark)

        assert inst.get_name() == "listed_building_harmonisation_process"

    def test__listed_building_harmonisation_process__process__renames_standardised_columns_and_adds_harmonised_fields(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame(
                [_standardised_row()],
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["endDate"] is None
        assert row["entryDate"] == "2024-01-01"
        assert row["listedBuildingGrade"] == "II"
        assert row["organisationEntity"] == "org-1"
        assert row["startDate"] == "2020-01-01"
        assert row["documentationUrl"] == "https://example.com/lb-001"
        assert row["dateReceived"] is not None
        assert row["validTo"] is None
        assert row["isActive"] == "Y"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__process__outputs_expected_harmonised_columns_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame(
                [_standardised_row()],
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "dataset",
            "endDate",
            "entity",
            "entryDate",
            "geometry",
            "listedBuildingGrade",
            "name",
            "organisationEntity",
            "point",
            "prefix",
            "reference",
            "startDate",
            "typology",
            "documentationUrl",
            "dateReceived",
            "rowID",
            "validTo",
            "isActive",
        ]

    def test__listed_building_harmonisation_process__process__calculates_rowid_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_row = _standardised_row()

        source_data = {
            "source_data": spark.createDataFrame(
                [source_row],
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("rowID").collect()[0]

        assert row["rowID"] == _expected_rowid_for_standardised_row(source_row)

    def test__listed_building_harmonisation_process__process__uses_dot_for_nulls_in_rowid_hash(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_row = _standardised_row(
            **{
                "end-date": None,
                "geometry": None,
                "organisation-entity": None,
                "point": None,
                "documentation-url": None,
            }
        )

        source_data = {
            "source_data": spark.createDataFrame(
                [source_row],
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.select("rowID").collect()[0]

        assert row["rowID"] == _expected_rowid_for_standardised_row(source_row)

    def test__listed_building_harmonisation_process__process__initial_load_uses_overwrite_and_inserts_all_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(entity="1001", reference="LB-001"),
            _standardised_row(entity="1002", reference="LB-002", name="Building Two"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert df.where(F.col("dateReceived").isNull()).count() == 0

    def test__listed_building_harmonisation_process__process__empty_source_returns_empty_output_on_initial_load(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_standardised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__process__preserves_duplicate_source_rows_on_initial_load_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(),
            _standardised_row(),
        ]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("reference") == "LB-001").count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__process__marks_old_active_row_inactive_when_same_entity_changes(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(
                entity="1001",
                reference="LB-001",
                name="Building One Updated",
            ),
        ]
        existing_rows = [_harmonised_row()]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_data": spark.createDataFrame(
                existing_rows,
                schema=_harmonised_schema(),
            ),
            "target_exists": True,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        old_row = (
            df.where((F.col("entity") == "1001") & (F.col("isActive") == "N"))
            .select("name", "validTo", "rowID")
            .collect()[0]
        )
        new_row = (
            df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y"))
            .select("name", "validTo", "rowID")
            .collect()[0]
        )

        assert old_row["name"] == "Old Building One"
        assert old_row["validTo"] is not None
        assert new_row["name"] == "Building One Updated"
        assert new_row["validTo"] is None
        assert old_row["rowID"] != new_row["rowID"]

        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "N")).count() == 1
        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_harmonisation_process__process__does_not_duplicate_when_same_entity_same_rowid_already_active(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_row = _standardised_row()
        expected_rowid = _expected_rowid_for_standardised_row(source_row)

        existing_rows = [
            _harmonised_row(
                name="Building One",
                rowID=expected_rowid,
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(
                [source_row],
                schema=_standardised_schema(),
            ),
            "target_data": spark.createDataFrame(
                existing_rows,
                schema=_harmonised_schema(),
            ),
            "target_exists": True,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        entity_rows = df.where(F.col("entity") == "1001")
        assert entity_rows.count() == 1
        assert entity_rows.where(F.col("isActive") == "Y").count() == 1
        assert entity_rows.where(F.col("isActive") == "N").count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__process__treats_new_reference_as_insert_like_legacy_counts(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(entity="1002", reference="LB-002", name="Building Two"),
        ]
        existing_rows = [_harmonised_row()]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_data": spark.createDataFrame(
                existing_rows,
                schema=_harmonised_schema(),
            ),
            "target_exists": True,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("reference") == "LB-002") & (F.col("isActive") == "Y")).count() == 1
        assert df.where((F.col("reference") == "LB-001") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_harmonisation_process__process__same_reference_different_entity_follows_legacy_merge_and_count_behaviour(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(entity="9999", reference="LB-001", name="Moved Entity"),
        ]
        existing_rows = [_harmonised_row()]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_data": spark.createDataFrame(
                existing_rows,
                schema=_harmonised_schema(),
            ),
            "target_exists": True,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).count() == 1
        assert df.where((F.col("entity") == "9999") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_harmonisation_process__process__rowid_changes_when_business_fields_change(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _standardised_row(name="Version A"),
            _standardised_row(name="Version B"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(
                source_rows,
                schema=_standardised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch(
            "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
        ):
            inst = ListedBuildingHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.select("name", "rowID").orderBy("name").collect()

        assert rows[0]["rowID"]
        assert rows[1]["rowID"]
        assert rows[0]["rowID"] != rows[1]["rowID"]