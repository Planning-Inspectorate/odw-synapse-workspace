import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.listed_building_curated_process import ListedBuildingCuratedProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


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


def _harmonised_schema_with_extra_source_column():
    return StructType(_harmonised_schema().fields + [StructField("sourceOnlyColumn", StringType(), True)])


def _curated_schema():
    return StructType(
        [
            StructField("entity", LongType(), True),
            StructField("reference", StringType(), True),
            StructField("name", StringType(), True),
            StructField("listedBuildingGrade", StringType(), True),
        ]
    )


def _curated_schema_with_extra_target_column():
    return StructType(
        [
            StructField("entity", LongType(), True),
            StructField("reference", StringType(), True),
            StructField("name", StringType(), True),
            StructField("listedBuildingGrade", StringType(), True),
            StructField("legacyOnlyColumn", StringType(), True),
        ]
    )


def _harmonised_row(**overrides):
    row = {
        "dataset": "listed-building",
        "endDate": None,
        "entity": "1001",
        "entryDate": "2024-01-01",
        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
        "listedBuildingGrade": "II",
        "name": "Building One",
        "organisationEntity": "org-1",
        "point": "POINT(1 1)",
        "prefix": "listed-building",
        "reference": "LB-001",
        "startDate": "2020-01-01",
        "typology": "grade-ii",
        "documentationUrl": "https://example.com/lb-001",
        "dateReceived": "2025-01-01",
        "rowID": "row-1",
        "validTo": None,
        "isActive": "Y",
    }
    row.update(overrides)
    return row


def _curated_row(**overrides):
    row = {
        "entity": 1001,
        "reference": "LB-001",
        "name": "Building One",
        "listedBuildingGrade": "II",
    }
    row.update(overrides)
    return row


class TestListedBuildingCuratedProcess(SparkTestCase):
    def test__listed_building_curated_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = ListedBuildingCuratedProcess(spark)

        assert inst.get_name() == "listed_building_curated_process"

    def test__listed_building_curated_process__process__projects_expected_curated_columns_from_active_harmonised_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                entity="1001",
                reference="LB-001",
                name="Building One",
                listedBuildingGrade="II",
                isActive="Y",
            ),
            _harmonised_row(
                entity="1002",
                reference="LB-002",
                name="Building Two",
                listedBuildingGrade="I",
                isActive="N",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert df.columns == ["entity", "reference", "name", "listedBuildingGrade"]
        assert len(rows) == 1
        assert rows[0]["entity"] == 1001
        assert rows[0]["reference"] == "LB-001"
        assert rows[0]["name"] == "Building One"
        assert rows[0]["listedBuildingGrade"] == "II"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__casts_entity_to_long(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame(
                [_harmonised_row(entity="1001")],
                schema=_harmonised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert dict(df.dtypes)["entity"] in ("bigint", "long")
        assert df.collect()[0]["entity"] == 1001

    def test__listed_building_curated_process__process__drops_duplicate_active_rows_via_distinct(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(entity="1001", rowID="row-1"),
            _harmonised_row(entity="1001", rowID="row-2"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__filters_null_entity_before_merge_logic(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(entity=None, reference="LB-NULL"),
            _harmonised_row(entity="1001", reference="LB-001"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("entity").isNull()).count() == 0
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__initial_load_uses_overwrite_and_inserts_all_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(entity="1001", reference="LB-001"),
            _harmonised_row(
                entity="1002",
                reference="LB-002",
                name="Building Two",
                listedBuildingGrade="I",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__empty_source_returns_empty_output_on_initial_load(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_harmonised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__new_entity_counts_as_insert_when_target_exists(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                entity="1002",
                reference="LB-002",
                name="Building Two",
                listedBuildingGrade="I",
            )
        ]
        target_rows = [_curated_row(entity=1001, reference="LB-001")]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_data": spark.createDataFrame(target_rows, schema=_curated_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("entity") == 1001).count() == 1
        assert df.where(F.col("entity") == 1002).count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__changed_non_key_fields_count_as_update_when_target_exists(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [_harmonised_row(entity="1001", name="Building One Updated")]
        target_rows = [_curated_row(entity=1001, name="Building One")]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_data": spark.createDataFrame(target_rows, schema=_curated_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.where(F.col("entity") == 1001).collect()[0]

        assert row["name"] == "Building One Updated"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_curated_process__process__identical_existing_row_produces_no_insert_or_update(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [_harmonised_row(entity="1001", name="Building One")]
        target_rows = [_curated_row(entity=1001, name="Building One")]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_data": spark.createDataFrame(target_rows, schema=_curated_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("entity") == 1001).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__process__aligns_to_common_columns_when_target_has_extra_columns(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [_harmonised_row(entity="1001", name="Building One Updated")]
        target_rows = [
            {
                "entity": 1001,
                "reference": "LB-001",
                "name": "Building One",
                "listedBuildingGrade": "II",
                "legacyOnlyColumn": "legacy",
            }
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_data": spark.createDataFrame(
                target_rows,
                schema=_curated_schema_with_extra_target_column(),
            ),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert "legacyOnlyColumn" not in df.columns
        assert df.where(F.col("entity") == 1001).collect()[0]["name"] == "Building One Updated"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_curated_process__process__ignores_source_only_columns_during_schema_alignment(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            {
                "dataset": "listed-building",
                "endDate": None,
                "entity": "1001",
                "entryDate": "2024-01-01",
                "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
                "listedBuildingGrade": "II",
                "name": "Building One Updated",
                "organisationEntity": "org-1",
                "point": "POINT(1 1)",
                "prefix": "listed-building",
                "reference": "LB-001",
                "startDate": "2020-01-01",
                "typology": "grade-ii",
                "documentationUrl": "https://example.com/lb-001",
                "dateReceived": "2025-01-01",
                "rowID": "row-1",
                "validTo": None,
                "isActive": "Y",
                "sourceOnlyColumn": "source-only",
            }
        ]

        source_df = spark.createDataFrame(
            source_rows,
            schema=_harmonised_schema_with_extra_source_column(),
        )

        target_df = spark.createDataFrame(
            [_curated_row(entity=1001, reference="LB-001", name="Building One", listedBuildingGrade="II")],
            schema=_curated_schema(),
        )

        source_data = {
            "source_data": source_df,
            "target_data": target_df,
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil"):
            inst = ListedBuildingCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert "sourceOnlyColumn" not in df.columns
        assert df.where(F.col("entity") == 1001).collect()[0]["name"] == "Building One Updated"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1
