import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process import (
    AppealAttributeMatrixHarmonisationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import DataFrame
from datetime import datetime
import mock

pytestmark = pytest.mark.skip(reason="Harmonisation logic not implemented yet")


class TestRefAppealAttributeMatrixHarmonisationProcess(ETLTestCase):
    def compare_data(self, expected: DataFrame, actual: DataFrame):
        uncomparable_cols = {"IngestionDate"}
        expected = expected.drop(*uncomparable_cols)
        actual = actual.drop(*uncomparable_cols)
        assert_dataframes_equal(expected, actual)

    def assert_harmonisation(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                (" Housing Need ", "APP-001", 1),
                ("Green Belt", "APP-002", 0),
            ],
            ["attribute", "appealReference", "s78"],
        )
        std_table = f"{test_case}_appeal_attribute_matrix"
        self.write_existing_table(
            spark,
            std_data,
            std_table,
            "odw_standardised_db",
            "odw-standardised",
            std_table,
            "overwrite",
        )
        output_table = f"{test_case}_ref_appeal_attribute_matrix"

        expected_data = spark.createDataFrame(
            (
                (
                    {
                        "attribute": "housing need",
                        "appealReference": "APP-001",
                        "s78": "1",
                        "TEMP_PK": "73f762918174260bb36e8bdc3d59e1c81c54a0ca8b5f41d1b3a053175bfef966",
                        "ODTSourceSystem": "AppealAttributeMatrix",
                        "IngestionDate": datetime(2026, 5, 13, 14, 19, 44, 976783),
                        "IsActive": "Y",
                    },
                    {
                        "attribute": "green belt",
                        "appealReference": "APP-002",
                        "s78": "0",
                        "TEMP_PK": "6ae990670184ea65724eba363b9438f56b0e2845d2f75eddb5d65181a63f1252",
                        "ODTSourceSystem": "AppealAttributeMatrix",
                        "IngestionDate": datetime(2026, 5, 13, 14, 19, 44, 976783),
                        "IsActive": "Y",
                    },
                )
            ),
            schema=StructType(
                [
                    StructField("attribute", StringType(), True),
                    StructField("appealReference", StringType(), True),
                    StructField("s78", StringType(), True),
                    StructField("TEMP_PK", StringType(), True),
                    StructField("ODTSourceSystem", StringType(), False),
                    StructField("IngestionDate", TimestampType(), False),
                    StructField("IsActive", StringType(), False),
                ]
            ),
        )

        with (
            mock.patch.object(
                AppealAttributeMatrixHarmonisationProcess,
                "STANDARDISED_TABLE",
                std_table,
            ),
            mock.patch.object(
                AppealAttributeMatrixHarmonisationProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="ref_appeal_attribute_matrix",
                orchestration_stage_name="harmonise",
            )
            assert_etl_result_successful(result)

        actual_data = spark.table(f"odw_harmonised_db.{output_table}")
        self.compare_data(expected_data, actual_data)

    def test__appeal_attribute_matrix_harmonisation_process__run__with_no_existing_data(
        self,
    ):
        self.assert_harmonisation("t_aamhp_r_wned")

    def test__appeal_attribute_matrix_harmonisation_process__run__with_existing_data(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_aamhp_r_wed"
        existing_data = spark.createDataFrame(
            (
                ("a",),
                ("b",),
            ),
            schema=StructType(
                [
                    StructField("attribute", StringType(), True),
                ]
            ),
        )
        output_table = f"{test_case}_ref_appeal_attribute_matrix"
        self.write_existing_table(
            spark,
            existing_data,
            output_table,
            "odw_harmonised_db",
            "odw-harmonised",
            output_table,
            "overwrite",
        )
        self.assert_harmonisation(test_case)
