import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql import DataFrame
from odw.core.etl.transformation.harmonised.listed_building_harmonisation_process import ListedBuildingHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful, assert_dataframes_equal
from datetime import date

pytestmark = pytest.mark.skip(reason="Harmonisation logic not implemented yet")


def _standardised_row(**overrides):
    row = {
        "dataset": "listed-building",
        "end-date": None,
        "entity": None,
        "entry-date": "2024-01-01",
        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
        "name": None,
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


def _harmonised_row(**overrides):
    row = {
        "dataset": "listed-building",
        "endDate": None,
        "entity": "1",
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
        "dateReceived": date(2026, 5, 8),
        "rowID": "4413ab3eda0f2857bfed7731cde4729c",
        "validTo": None,
        "isActive": "Y",
    }
    row.update(overrides)
    return row


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


class TestListedBuildingHarmonisationProcess(ETLTestCase):
    def compare_harmonised_data(expected_data: DataFrame, actual_data: DataFrame):
        uncomparable_cols = "dateReceived"
        expected_data_cleaned = expected_data.drop(*uncomparable_cols)
        actual_data_cleaned = actual_data.drop(*uncomparable_cols)
        assert_dataframes_equal(expected_data_cleaned, actual_data_cleaned)

    def test__listed_building_harmonisation_process__run__with_no_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_lbhp_r_wned"
        listed_building = spark.createDataFrame(
            [
                _standardised_row(entity=1, name="Building One"),
                _standardised_row(entity=2, name="Building Two"),
                _standardised_row(entity=3, name="Building Three"),
            ],
            schema=_standardised_schema(),
        )
        listed_building_table_name = f"{test_case}_listed_building"
        self.write_existing_table(
            spark, listed_building, listed_building_table_name, "odw_standardised_db", "odw-standardised", listed_building_table_name, "overwrite"
        )
        expected_harmonised_listed_building = spark.createDataFrame(
            [
                _harmonised_row(entity=1, name="Building One"),
                _harmonised_row(entity=2, name="Building Two"),
                _harmonised_row(entity=3, name="Building Three"),
            ],
            schema=_harmonised_schema(),
        )
        with mock.patch.object(ListedBuildingHarmonisationProcess, "OUTPUT_TABLE", listed_building_table_name):
            inst = ListedBuildingHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="listed_building", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_harmonised_db.{listed_building_table_name}")
            self.compare_harmonised_data(expected_harmonised_listed_building, actual_table_data)

    def test__listed_building_harmonisation_process__run__with_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_lbhp_r_wed"
        listed_building = spark.createDataFrame(
            [
                _standardised_row(entity=1, name="Building One"),  # Should be updated
                _standardised_row(entity=2, name="Building Two"),  # Should not be modified
                _standardised_row(entity=3, name="Building Three"),  # Should be inserted
            ],
            schema=_standardised_schema(),
        )
        listed_building_table_name = f"{test_case}_listed_building"
        self.write_existing_table(
            spark,
            listed_building,
            listed_building_table_name,
            "odw_standardised_db",
            "odw-standardised",
            listed_building_table_name,
            "overwrite",
        )
        existing_harmonised_data = spark.createDataFrame(
            (
                _harmonised_row(entity=1, name="Building Old Name", rowID="4413ab3eda0f2857bfed7731cde4729c"),  # Should be updated
                _harmonised_row(entity=2, name="Building Two", rowID="390b65b99f173831af260bef26accc90"),  # Should not be modified
                _harmonised_row(entity=4, name="Building Four", rowID="someGuid"),  # Should be marked as inactive
            ),
            schema=_harmonised_schema(),
        )
        self.write_existing_table(
            spark,
            existing_harmonised_data,
            listed_building_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            listed_building_table_name,
            "overwrite",
        )
        # Note: The existing notebook logic does not seem to handle deactivating existing records. I.e. We should expected entity=4
        #       to be deactivated, but this does not seem to happen in the legacy notebook. This should be reviewed
        # Generated by running the original notebook
        expected_harmonised_listed_building = spark.createDataFrame(
            [
                _harmonised_row(entity=1, name="Building One", rowID="4413ab3eda0f2857bfed7731cde4729c"),  # Should be updated
                _harmonised_row(entity=2, name="Building Two", rowID="390b65b99f173831af260bef26accc90"),  # Should not be modified
                _harmonised_row(entity=3, name="Building Three", rowID="d31d10cb03b0a874acfc8bac9a1f0397"),  # Should be inserted
                _harmonised_row(entity=4, name="Building Four", rowID="someGuid"),  # Should not be modified
            ],
            schema=_harmonised_schema(),
        )
        with mock.patch.object(ListedBuildingHarmonisationProcess, "OUTPUT_TABLE", listed_building_table_name):
            inst = ListedBuildingHarmonisationProcess(spark)
            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="listed_building", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_harmonised_db.{listed_building_table_name}")
            self.compare_harmonised_data(expected_harmonised_listed_building, actual_table_data)
