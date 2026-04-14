import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.listed_building_harmonisation_process import ListedBuildingHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
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


def _standardised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "II",
            ),
            (
                "listed-building",
                None,
                "1002",
                "2024-02-01",
                None,
                "Building Two",
                "org-2",
                None,
                "listed-building",
                "LB-002",
                "2021-01-01",
                "grade-i",
                None,
                "I",
            ),
        ],
        schema=_standardised_schema(),
    )


def _changed_standardised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Building One Updated",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "II",
            ),
        ],
        schema=_standardised_schema(),
    )


def _identical_standardised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Old Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "II",
            ),
        ],
        schema=_standardised_schema(),
    )


def _duplicate_standardised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "II",
            ),
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "II",
            ),
        ],
        schema=_standardised_schema(),
    )


def _same_reference_different_entity_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "9999",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "Different Entity Same Reference",
                "org-9",
                "POINT(9 9)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-999",
                "II",
            ),
        ],
        schema=_standardised_schema(),
    )


def _empty_standardised_df(spark):
    return spark.createDataFrame([], schema=_standardised_schema())


def _existing_harmonised_df(spark):
    return spark.createDataFrame(
        [
            (
                "listed-building",
                None,
                "1001",
                "2024-01-01",
                "POLYGON((1 1,2 2,3 3,1 1))",
                "II",
                "Old Building One",
                "org-1",
                "POINT(1 1)",
                "listed-building",
                "LB-001",
                "2020-01-01",
                "grade-ii",
                "https://example.com/lb-001",
                "2025-01-01",
                "old-row-id",
                None,
                "Y",
            ),
        ],
        schema=_harmonised_schema(),
    )


class TestListedBuildingHarmonisationProcess(ETLTestCase):
    def test__listed_building_harmonisation_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _standardised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

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

        expected_columns = [
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
        assert df.columns == expected_columns

        row = (
            df.where(F.col("entity") == "1001")
            .select(
                "dataset",
                "endDate",
                "entryDate",
                "listedBuildingGrade",
                "organisationEntity",
                "documentationUrl",
                "dateReceived",
                "isActive",
                "validTo",
                "rowID",
            )
            .collect()[0]
        )

        assert row["dataset"] == "listed-building"
        assert row["endDate"] is None
        assert row["entryDate"] == "2024-01-01"
        assert row["listedBuildingGrade"] == "II"
        assert row["organisationEntity"] == "org-1"
        assert row["documentationUrl"] == "https://example.com/lb-001"
        assert row["dateReceived"] is not None
        assert row["isActive"] == "Y"
        assert row["validTo"] is None
        assert row["rowID"]

        assert df.where(F.col("dateReceived").isNull()).count() == 0

    def test__listed_building_harmonisation_process__run__deactivates_old_version_and_inserts_new_version_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": _changed_standardised_df(spark),
            "target_data": _existing_harmonised_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
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

    def test__listed_building_harmonisation_process__run__inserts_new_reference_and_keeps_existing_active_row(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        new_only_source = _standardised_df(spark).where(F.col("entity") == "1002")

        source_data = {
            "source_data": new_only_source,
            "target_data": _existing_harmonised_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

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

        source_data = {
            "source_data": _identical_standardised_df(spark),
            "target_data": _existing_harmonised_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

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

        source_data = {
            "source_data": _empty_standardised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

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

        source_data = {
            "source_data": _duplicate_standardised_df(spark),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

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

        source_data = {
            "source_data": _same_reference_different_entity_df(spark),
            "target_data": _existing_harmonised_df(spark),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.harmonised.listed_building_harmonisation_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = ListedBuildingHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("entity") == "1001") & (F.col("isActive") == "Y")).count() == 1
        assert df.where((F.col("entity") == "9999") & (F.col("isActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 1