import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.listed_building_curated_process import ListedBuildingCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

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


def _curated_schema():
    return StructType(
        [
            StructField("entity", LongType(), True),
            StructField("reference", StringType(), True),
            StructField("name", StringType(), True),
            StructField("listedBuildingGrade", StringType(), True),
        ]
    )


def _harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "II",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "2025-01-01",
                "row-1",
                None,
                "Y",
            ),
            (
                "listed-building",
                None,
                "1002",
                "2024-02-01",
                None,
                "I",
                "Building Two",
                "org-2",
                None,
                "listed-building",
                "LB-002",
                "2021-01-01",
                "grade-i",
                None,
                "2025-01-01",
                "row-2",
                None,
                "Y",
            ),
            (
                "listed-building",
                None,
                "1003",
                "2024-03-01",
                None,
                "II*",
                "Inactive Building",
                "org-3",
                None,
                "listed-building",
                "LB-003",
                "2022-01-01",
                "grade-ii-star",
                None,
                "2025-01-01",
                "row-3",
                None,
                "N",
            ),
        ],
        schema=_harmonised_schema(),
    )


def _changed_harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "II",
                "Building One Updated",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "2025-01-01",
                "row-9",
                None,
                "Y",
            ),
        ],
        schema=_harmonised_schema(),
    )


def _duplicate_harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "II",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "2025-01-01",
                "row-1",
                None,
                "Y",
            ),
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "II",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "2025-01-01",
                "row-2",
                None,
                "Y",
            ),
        ],
        schema=_harmonised_schema(),
    )


def _empty_harmonised_df(spark):
    return spark.createDataFrame([], schema=_harmonised_schema())


def _existing_curated_df(spark):
    return spark.createDataFrame(
        [
            (1001, "LB-001", "Building One", "II"),
        ],
        schema=_curated_schema(),
    )


class TestListedBuildingCuratedProcess(ETLTestCase):
    def test__listed_building_curated_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _harmonised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

        assert df.columns == ["entity", "reference", "name", "listedBuildingGrade"]

        entity_ids = {row["entity"] for row in df.select("entity").collect()}
        assert entity_ids == {1001, 1002}
        assert df.where(F.col("entity") == 1003).count() == 0

    def test__listed_building_curated_process__run__updates_existing_entity_when_non_key_fields_change(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _changed_harmonised_df(spark),
            "target_data": _existing_curated_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        row = df.where(F.col("entity") == 1001).collect()[0]

        assert row["name"] == "Building One Updated"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1

    def test__listed_building_curated_process__run__does_not_duplicate_identical_existing_entity(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        identical_source = spark.createDataFrame(
            [
                (
                    "listed-building",
                    None,
                    "1001",
                    "2024-01-01",
                    "POLYGON((1 1,2 2,3 3,1 1))",
                    "II",
                    "Building One",
                    "org-1",
                    "POINT(1 1)",
                    "listed-building",
                    "LB-001",
                    "2020-01-01",
                    "grade-ii",
                    "https://example.com/lb-001",
                    "2025-01-01",
                    "row-1",
                    None,
                    "Y",
                ),
            ],
            schema=_harmonised_schema(),
        )

        source_data = {
            "source_data": identical_source,
            "target_data": _existing_curated_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("entity") == 1001).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__run__drops_duplicate_source_rows_and_filters_inactive_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _duplicate_harmonised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("entity") == 1001).count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__listed_building_curated_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _empty_harmonised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.listed_building_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
