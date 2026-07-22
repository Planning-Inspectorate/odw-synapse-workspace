import mock
import pytest
import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import StringType, StructField, StructType
from odw.core.etl.transformation.standardised.listed_building_standardisation_process import (
    ListedBuildingStandardisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_etl_result_successful,
    assert_dataframes_equal,
)

pytestmark = pytest.mark.skip(reason="Standardisation logic not implemented yet")


def _listed_building_rows():
    return [
        {
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
            "start-date": "2025-01-01",
            "typology": "grade-ii",
            "documentation-url": "https://example.com/lb-001",
            "listed-building-grade": "II",
        },
        {
            "dataset": "listed-building",
            "end-date": "2025-01-01",
            "entity": "1002",
            "entry-date": "2024-02-01",
            "geometry": None,
            "name": "Building Two",
            "organisation-entity": "org-2",
            "point": None,
            "prefix": "listed-building",
            "reference": "LB-002",
            "start-date": "2025-01-01",
            "typology": "grade-i",
            "documentation-url": None,
            "listed-building-grade": "I",
        },
    ]


def _listed_building_entity_schema():
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


def _listed_building_outline_entity_rows():
    return [
        {
            "address": "1 High Street",
            "address-text": "1 High Street, Town",
            "dataset": "listed-building-outline",
            "document-url": "https://example.com/doc-1",
            "documentation-url": "https://example.com/outline-1",
            "end-date": None,
            "entity": "2001",
            "entry-date": "2025-01-01",
            "geometry": "POLYGON((4 4,5 5,6 6,4 4))",
            "listed-building": "1001",
            "name": "Outline One",
            "notes": "Some notes",
            "organisation-entity": "org-1",
            "point": "POINT(4 4)",
            "prefix": "listed-building-outline",
            "reference": "LBO-001",
            "start-date": "2025-01-01",
            "typology": "outline",
        },
        {
            "address": None,
            "address-text": None,
            "dataset": "listed-building-outline",
            "document-url": None,
            "documentation-url": None,
            "end-date": "2025-06-01",
            "entity": "2002",
            "entry-date": "2024-04-01",
            "geometry": None,
            "listed-building": "1002",
            "name": "Outline Two",
            "notes": None,
            "organisation-entity": "org-2",
            "point": None,
            "prefix": "listed-building-outline",
            "reference": "LBO-002",
            "start-date": "2023-01-01",
            "typology": "outline",
        },
    ]


def _listed_building_outline_entity_schema():
    return StructType(
        [
            StructField("address", StringType(), True),
            StructField("address-text", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("document-url", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("listed-building", StringType(), True),
            StructField("name", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
        ]
    )


class TestListedBuildingStandardisationProcess(ETLTestCase):
    def assert_run(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()
        listed_building_table_name = f"{test_case}_listed_building"
        listed_building_outline_table_name = f"{listed_building_table_name}_outline"
        data_folder = "2025-01-01"
        # Write raw listed building data
        listed_building_file_name = f"{listed_building_table_name}.json"
        listed_building = [{"entities": _listed_building_rows()}]
        self.write_json(
            listed_building,
            ["odw-raw", "ListedBuildings", data_folder, listed_building_file_name],
        )
        # Write raw listed building outline data
        listed_building_outline_file_name = f"{listed_building_outline_table_name}.json"
        listed_building_outline = [{"entities": _listed_building_outline_entity_rows()}]
        self.write_json(
            listed_building_outline,
            [
                "odw-raw",
                "ListedBuildings",
                data_folder,
                listed_building_outline_file_name,
            ],
        )

        expected_standardised_listed_bulding = spark.createDataFrame(
            _listed_building_rows(), schema=_listed_building_entity_schema()
        )
        expected_standardised_listed_bulding_outline = spark.createDataFrame(
            _listed_building_outline_entity_rows(),
            schema=_listed_building_outline_entity_schema(),
        )
        with mock.patch.object(
            ListedBuildingStandardisationProcess,
            "OUTPUT_TABLE",
            listed_building_table_name,
        ):
            inst = ListedBuildingStandardisationProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="listed_building",
                orchestration_stage_name="standardise",
            )
            assert_etl_result_successful(result)
            actual_standardised_listed_building = spark.table(
                f"odw_standardised_db.{listed_building_table_name}"
            )
            assert_dataframes_equal(
                expected_standardised_listed_bulding,
                actual_standardised_listed_building,
            )
            actual_standardised_listed_building_outline = spark.table(
                f"odw_standardised_db.{listed_building_outline_table_name}"
            )
            assert_dataframes_equal(
                expected_standardised_listed_bulding_outline,
                actual_standardised_listed_building_outline,
            )

    def test__listed_building_standardisation_process__run__with_no_existing_data(self):
        """
        - Given I have raw listed building and listed building outline data
        - When I call ListedBuildingStandardisationProcess.run
        - Then the listed_building and listed_building_outline tables should be created in the standardised layer
        """
        self.assert_run("t_lbsp_r_wned")

    def test__listed_building_standardisation_process__run__with_existing_data(self):
        """
        - Given I have raw listed building and listed building outline data and I have some existing standardised data
        - When I call ListedBuildingStandardisationProcess.run
        - Then the listed_building and listed_building_outline tables should overwrite the existing standardised data
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_lbsp_r_wned"
        listed_building_table_name = f"{test_case}_listed_building"
        listed_building_outline_table_name = f"{listed_building_table_name}_outline"
        existing_standardised_listed_bulding = spark.createDataFrame(
            (
                dict(),  # A row that is all null
            ),
            schema=_listed_building_entity_schema(),
        )
        existing_standardised_listed_bulding.show()
        self.write_existing_table(
            spark,
            existing_standardised_listed_bulding,
            listed_building_table_name,
            "odw_standardised_db",
            "odw-standardised",
            listed_building_table_name,
            "overwrite",
        )
        existing_standardised_listed_bulding_outline = spark.createDataFrame(
            (
                dict(),  # A row that is all null
            ),
            schema=_listed_building_outline_entity_schema(),
        )
        self.write_existing_table(
            spark,
            existing_standardised_listed_bulding_outline,
            listed_building_outline_table_name,
            "odw_standardised_db",
            "odw-standardised",
            listed_building_outline_table_name,
            "overwrite",
        )
        self.assert_run(test_case)
