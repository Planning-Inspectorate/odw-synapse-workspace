import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.standardised.appeal_attribute_matrix_standardisation_process import AppealAttributeMatrixStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import mock
import pyspark.sql.types as T

pytestmark = pytest.mark.skip(reason="Standardisation logic not implemented yet")


class TestRefAppealAttributeMatrixStandardisationProcess(ETLTestCase):
    def assert_standardisation(self, test_case: str):
        spark = PytestSparkSessionUtil().get_spark_session()

        data_folder = f"{test_case}_AppealAttributeMatrix"
        date_folder = "2027-02-01"
        raw_csv_data = (
            ("attribute", "Appeal Reference", "S78", "_c0", ""),
            ("Housing Need", "APP-001", 1, "ignore", "drop-blank"),
            ("", "APP-002", 0, "ignore", "drop-blank"),
            ("   ", "APP-003", 1, "ignore", "drop-blank"),
            ("Green Belt", "APP-004", 0, "ignore", "drop-blank"),
            (None, "APP-005", 1, "ignore", "drop-blank"),
        )
        self.write_csv(raw_csv_data, ["odw-raw", data_folder, date_folder, "appeal-attribute-matrix.csv"])

        output_table = f"{test_case}_appeal_attribute_matrix"

        with (
            mock.patch.object(AppealAttributeMatrixStandardisationProcess, "CSV_FOLDER", output_table),
            mock.patch.object(AppealAttributeMatrixStandardisationProcess, "OUTPUT_TABLE", output_table),
        ):
            inst = AppealAttributeMatrixStandardisationProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case, orchestration_entity_name="ref_appeal_attrbute_matrix", orchestration_stage_name="standardise"
            )
            assert_etl_result_successful(result)

        expected_data = spark.createDataFrame(
            (
                {"attribute": "Housing Need", "appealReference": "APP-001", "s78": "1"},
                {"attribute": "Green Belt", "appealReference": "APP-004", "s78": "0"},
            ),
            schema=T.StructType(
                [
                    T.StructField("attribute", T.StringType(), True),
                    T.StructField("appealReference", T.StringType(), True),
                    T.StructField("s78", T.StringType(), True),
                ]
            ),
        )
        actual_data = spark.table(f"odw_standardised_db.{output_table}")
        assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_attribute_matrix_standardisation_process__run__with_no_existing_data(self):
        self.assert_standardisation("t_aamsp_r_wned")

    def test__appeal_attribute_matrix_standardisation_process__run__with_existing_data(self):
        test_case = "t_aamsp_r_wned"
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_data = spark.createDataFrame(
            ({"attribute": "Old record", "appealReference": "old-ref", "s78": "1"},),
            schema=T.StructType(
                [
                    T.StructField("attribute", T.StringType(), True),
                    T.StructField("appealReference", T.StringType(), True),
                    T.StructField("s78", T.StringType(), True),
                ]
            ),
        )
        output_table = f"{test_case}_appeal_attribute_matrix"
        self.write_existing_table(spark, existing_data, output_table, "odw_standardised_db", "odw-standardised", output_table, "overwrite")
        self.assert_standardisation("t_aamsp_r_wed")
