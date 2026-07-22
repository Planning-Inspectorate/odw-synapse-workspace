import pytest
import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process import (
    AppealAttributeMatrixCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from pyspark.sql.types import StructType, StructField, StringType
import mock

pytestmark = pytest.mark.skip(reason="Curated logic not implemented yet")


class TestRefAppealAttributeMatrixCurationProcess(ETLTestCase):
    def assert_curation(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [
                ("a", "Y"),
                ("b", "N"),
            ],
            ["attribute", "IsActive"],
        )
        hrm_table = f"{test_case}_ref_appeal_attribute_matrix"
        self.write_existing_table(
            spark,
            hrm_data,
            hrm_table,
            "odw_harmonised_db",
            "odw-harmonised",
            hrm_table,
            "overwrite",
        )

        std_data = spark.createDataFrame(
            [("a",), ("b",)],
            ["attribute"],
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
                ("a", "Y"),
                ("b", "N"),
            ),
            schema=StructType(
                [
                    StructField("attribute", StringType(), True),
                    StructField("IsActive", StringType(), True),
                ]
            ),
        )

        with (
            mock.patch.object(
                AppealAttributeMatrixCuratedProcess, "STANDARDISED_TABLE", std_table
            ),
            mock.patch.object(
                AppealAttributeMatrixCuratedProcess, "HARMONISED_TABLE", hrm_table
            ),
            mock.patch.object(
                AppealAttributeMatrixCuratedProcess, "OUTPUT_TABLE", output_table
            ),
        ):
            inst = AppealAttributeMatrixCuratedProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="ref_appeal_attribute_matrix",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)
            actual_table_data = spark.table(output_table)
            assert_dataframes_equal(expected_data, actual_table_data)

    def test__appeal_attribute_matrix_curated_process__run__with_no_existing_data(self):
        self.assert_curation("t_aamcp_r_wned")

    def test__appeal_attribute_matrix_curated_process__run__with_existing_data(self):
        test_case = "t_aamcp_r_faaml"
        spark = PytestSparkSessionUtil().get_spark_session()
        expected_data = spark.createDataFrame(
            (
                ("a", "Y", "extraA"),
                ("b", "N", "extraA"),
            ),
            schema=StructType(
                [
                    StructField("attribute", StringType(), True),
                    StructField("IsActive", StringType(), True),
                    StructField("extraAttribute", StringType(), True),
                ]
            ),
        )
        output_table = f"{test_case}_ref_appeal_attribute_matrix"
        self.write_existing_table(
            spark,
            expected_data,
            output_table,
            "odw_curated_db",
            "odw-curated",
            output_table,
            "overwrite",
        )
        self.assert_curation("t_aamcp_r_wed")
