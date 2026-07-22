import mock
import pytest
import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.listed_building_curated_process import (
    ListedBuildingCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)

pytestmark = pytest.mark.skip(reason="Curated logic not implemented yet")


def _harmonised_row(**overrides):
    base = {
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
        "validTo": "None",
        "isActive": "Y",
    }
    return base | overrides


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


class TestListedBuildingCuratedProcess(ETLTestCase):
    def test__listed_building_curated_process__run__with_no_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_lbcpr_wned"
        harmonised_data = spark.createDataFrame(
            (
                _harmonised_row(
                    name="Building One",
                    entity="1001",
                    reference="LB-001",
                    listedBuildingGrade="I",
                ),
                _harmonised_row(
                    name="Building Two",
                    entity="1002",
                    reference="LB-002",
                    listedBuildingGrade="I",
                ),
            ),
            schema=_harmonised_schema(),
        )
        table_name = f"{test_case}_listed_building"
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            table_name,
            "overwrite",
        )
        expected_curated_data_after_writing = spark.createDataFrame(
            (
                {
                    "entity": 1001,
                    "reference": "LB-001",
                    "name": "Building One",
                    "listedBuildingGrade": "I",
                },
                {
                    "entity": 1002,
                    "reference": "LB-002",
                    "name": "Building Two",
                    "listedBuildingGrade": "I",
                },
            ),
            schema=_curated_schema(),
        )
        with mock.patch.object(
            ListedBuildingCuratedProcess, "OUTPUT_TABLE", table_name
        ):
            inst = ListedBuildingCuratedProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="listed_building",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_curated_db.{table_name}")
            assert_dataframes_equal(
                expected_curated_data_after_writing, actual_table_data
            )

    def test__listed_building_curated_process__run__with_existing_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_lbcpr_wed"
        harmonised_data = spark.createDataFrame(
            (
                _harmonised_row(
                    name="Building One Renamed",
                    entity="1001",
                    reference="LB-001 Updated",
                    listedBuildingGrade="I Updated",
                ),  # Should be updated
                _harmonised_row(
                    name="Building Two",
                    entity="1002",
                    reference="LB-002",
                    listedBuildingGrade="I",
                ),
            ),
            schema=_harmonised_schema(),
        )
        table_name = f"{test_case}_listed_building"
        self.write_existing_table(
            spark,
            harmonised_data,
            table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            table_name,
            "overwrite",
        )
        curated_data = spark.createDataFrame(
            (
                {
                    "entity": 1001,
                    "reference": "LB-001",
                    "name": "Building One",
                    "listedBuildingGrade": "I",
                },  # Should be updated
            ),
            schema=_curated_schema(),
        )
        self.write_existing_table(
            spark,
            curated_data,
            table_name,
            "odw_curated_db",
            "odw-curated",
            table_name,
            "overwrite",
        )
        expected_curated_data_after_writing = spark.createDataFrame(
            (
                {
                    "entity": 1001,
                    "reference": "LB-001 Updated",
                    "name": "Building One Renamed",
                    "listedBuildingGrade": "I Updated",
                },  # Updated
                {
                    "entity": 1002,
                    "reference": "LB-002",
                    "name": "Building Two",
                    "listedBuildingGrade": "I",
                },
            ),
            schema=_curated_schema(),
        )
        with mock.patch.object(
            ListedBuildingCuratedProcess, "OUTPUT_TABLE", table_name
        ):
            inst = ListedBuildingCuratedProcess(spark)
            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="listed_building",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)
            actual_table_data = spark.table(f"odw_curated_db.{table_name}")
            assert_dataframes_equal(
                expected_curated_data_after_writing, actual_table_data
            )
