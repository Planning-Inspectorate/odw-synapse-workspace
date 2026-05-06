import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process import AppealAttributeMatrixCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import mock

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


class TestRefAppealAttributeMatrixCurationProcess(ETLTestCase):
    def test__appeal_attribute_matrix_curated_process__run__filters_active_and_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [
                ("a", "Y"),
                ("b", "N"),
            ],
            ["attribute", "IsActive"],
        )

        std_data = spark.createDataFrame(
            [("a",), ("b",)],
            ["attribute"],
        )

        source_data = {
            "harmonised_data": hrm_data,
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["attribute"] == "a"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_curated_process__run__isactive_branch_takes_precedence_over_latest_per_temp_pk(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [
                ("a-old", "pk1", "2025-01-01", "Y"),
                ("a-new", "pk1", "2025-02-01", "N"),
            ],
            ["attribute", "TEMP_PK", "IngestionDate", "IsActive"],
        )

        std_data = spark.createDataFrame(
            [("a",)],
            ["attribute"],
        )

        source_data = {
            "harmonised_data": hrm_data,
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["attribute"] == "a-old"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_curated_process__run__selects_latest_per_temp_pk_when_no_isactive(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [
                ("a", "pk1", "2025-01-01"),
                ("a", "pk1", "2025-02-01"),
            ],
            ["attribute", "TEMP_PK", "IngestionDate"],
        )

        std_data = spark.createDataFrame(
            [("a",)],
            ["attribute"],
        )

        source_data = {
            "harmonised_data": hrm_data,
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.collect()[0]["IngestionDate"] == "2025-02-01"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_curated_process__run__passes_through_when_no_isactive_and_no_temp_pk(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [("a",), ("b",)],
            ["attribute"],
        )

        std_data = spark.createDataFrame(
            [("a",), ("b",)],
            ["attribute"],
        )

        source_data = {
            "harmonised_data": hrm_data,
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixCuratedProcess(spark)

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

    def test__appeal_attribute_matrix_curated_process__run__adds_missing_standardised_columns_orders_std_first_then_extras_and_casts_s78(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        hrm_data = spark.createDataFrame(
            [("a", "extra", 1)],
            ["attribute", "TEMP_PK", "s78"],
        )

        std_data = spark.createDataFrame(
            [("a", "APP-001")],
            ["attribute", "appealReference"],
        )

        source_data = {
            "harmonised_data": hrm_data,
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.curated.appeal_attribute_matrix_curated_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert df.columns == ["attribute", "appealReference", "TEMP_PK", "s78"]
        assert dict(df.dtypes)["appealReference"] == "string"
        assert row["appealReference"] is None
        assert dict(df.dtypes)["s78"] == "string"
        assert row["s78"] == "1"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1
