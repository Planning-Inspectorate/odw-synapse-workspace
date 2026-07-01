import pytest
from odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process import AppealAttributeMatrixHarmonisationProcess
from odw.core.etl.metadata_manager import MetadataManager
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
from pyspark.sql import functions as F

pytestmark = pytest.mark.skip(reason="Harmonisation logic not implemented yet")


class TestRefAppealAttributeMatrixHarmonisationProcess(SparkTestCase):
    def test__appeal_attribute_matrix_harmonisation_process__process__trims_all_string_columns_and_normalises_attribute(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("  Housing Need  ", " APP-001 ", " 1 ", "  custom-source  ", " Y "),
            ],
            ["attribute", "appealReference", "s78", "ODTSourceSystem", "IsActive"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["attribute"] == "housing need"
        assert row["appealReference"] == "APP-001"
        assert row["s78"] == "1"
        assert row["ODTSourceSystem"] == "custom-source"
        assert row["IsActive"] == "Y"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_harmonisation_process__process__generates_temp_pk_using_legacy_sha256_formula(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("  Housing Need  ",),
            ],
            ["attribute"],
        )

        expected_hash = (
            spark.createDataFrame([("housing need",)], ["attribute"])
            .select(F.sha2(F.to_json(F.struct(F.col("attribute"))), 256).alias("TEMP_PK"))
            .collect()[0]["TEMP_PK"]
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        actual_hash = df.collect()[0]["TEMP_PK"]

        assert actual_hash == expected_hash

    def test__appeal_attribute_matrix_harmonisation_process__process__adds_default_columns_when_missing(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need",),
            ],
            ["attribute"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["ODTSourceSystem"] == "AppealAttributeMatrix"
        assert row["IsActive"] == "Y"
        assert row["IngestionDate"] is not None

    def test__appeal_attribute_matrix_harmonisation_process__process__preserves_existing_odt_source_system_and_isactive_when_present(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", "ManualLoad", "N"),
            ],
            ["attribute", "ODTSourceSystem", "IsActive"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["ODTSourceSystem"] == "ManualLoad"
        assert row["IsActive"] == "N"

    def test__appeal_attribute_matrix_harmonisation_process__process__casts_existing_ingestion_date_to_timestamp(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", "2025-01-01"),
            ],
            ["attribute", "IngestionDate"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert dict(df.dtypes)["IngestionDate"] == "timestamp"

    def test__appeal_attribute_matrix_harmonisation_process__process__keeps_original_column_order_and_appends_extras(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", "APP-001"),
            ],
            ["attribute", "appealReference"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "attribute",
            "appealReference",
            "TEMP_PK",
            "ODTSourceSystem",
            "IngestionDate",
            "IsActive",
        ]

    def test__appeal_attribute_matrix_harmonisation_process__process__keeps_original_column_order_when_ingestion_date_exists_in_source(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", "APP-001", "2025-01-01"),
            ],
            ["attribute", "appealReference", "IngestionDate"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "attribute",
            "appealReference",
            "IngestionDate",
            "TEMP_PK",
            "ODTSourceSystem",
            "IsActive",
        ]

    def test__appeal_attribute_matrix_harmonisation_process__process__casts_s78_to_string_if_present(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", 1),
            ],
            ["attribute", "s78"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert dict(df.dtypes)["s78"] == "string"

    def test__appeal_attribute_matrix_harmonisation_process__process__preserves_duplicates_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need",),
                ("housing need",),
            ],
            ["attribute"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data={"standardised_data": std_data})

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_attribute_matrix_harmonisation_process__process__uses_overwrite_write_mode(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need",),
            ],
            ["attribute"],
        )

        with mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil"):
            inst = AppealAttributeMatrixHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data={"standardised_data": std_data})

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_harmonisation_process__run__end_to_end_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                (" Housing Need ", "APP-001", 1),
                ("Green Belt", "APP-002", 0),
            ],
            ["attribute", "appealReference", "s78"],
        )

        source_data = {
            "standardised_data": std_data,
        }

        expected_hash = (
            spark.createDataFrame([("housing need",)], ["attribute"])
            .select(F.sha2(F.to_json(F.struct(F.col("attribute"))), 256).alias("TEMP_PK"))
            .collect()[0]["TEMP_PK"]
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(MetadataManager, "__init__", return_value=None),
                mock.patch.object(MetadataManager, "create", return_value=None),
                mock.patch.object(MetadataManager, "update", return_value=None),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        rows = {row["attribute"]: row.asDict(recursive=True) for row in df.collect()}

        assert df.count() == 2
        assert rows["housing need"]["attribute"] == "housing need"
        assert rows["housing need"]["appealReference"] == "APP-001"
        assert rows["housing need"]["s78"] == "1"
        assert rows["housing need"]["TEMP_PK"] == expected_hash
        assert rows["housing need"]["ODTSourceSystem"] == "AppealAttributeMatrix"
        assert rows["housing need"]["IsActive"] == "Y"
        assert dict(df.dtypes)["s78"] == "string"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 2

    def test__appeal_attribute_matrix_harmonisation_process__run__handles_missing_optional_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need",),
            ],
            ["attribute"],
        )

        source_data = {
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(MetadataManager, "__init__", return_value=None),
                mock.patch.object(MetadataManager, "create", return_value=None),
                mock.patch.object(MetadataManager, "update", return_value=None),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        row = df.collect()[0]

        assert row["ODTSourceSystem"] == "AppealAttributeMatrix"
        assert row["IsActive"] == "Y"
        assert row["IngestionDate"] is not None
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_harmonisation_process__run__preserves_existing_odt_source_system_isactive_and_casts_existing_ingestion_date(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                (" Housing Need ", " LegacySource ", " N ", "2025-01-01", 1),
            ],
            ["attribute", "ODTSourceSystem", "IsActive", "IngestionDate", "s78"],
        )

        source_data = {
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(MetadataManager, "__init__", return_value=None),
                mock.patch.object(MetadataManager, "create", return_value=None),
                mock.patch.object(MetadataManager, "update", return_value=None),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["attribute"] == "housing need"
        assert row["ODTSourceSystem"] == "LegacySource"
        assert row["IsActive"] == "N"
        assert row["s78"] == "1"
        assert dict(df.dtypes)["IngestionDate"] == "timestamp"
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 1

    def test__appeal_attribute_matrix_harmonisation_process__run__preserves_duplicates_and_column_order_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        std_data = spark.createDataFrame(
            [
                ("housing need", "APP-001"),
                ("housing need", "APP-001"),
            ],
            ["attribute", "appealReference"],
        )

        source_data = {
            "standardised_data": std_data,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as MockEtlLogging,
            mock.patch("odw.core.etl.transformation.harmonised.appeal_attribute_matrix_harmonisation_process.LoggingUtil") as MockProcessLogging,
        ):
            MockEtlLogging.return_value = mock.Mock()
            MockProcessLogging.return_value = mock.Mock()

            inst = AppealAttributeMatrixHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(MetadataManager, "__init__", return_value=None),
                mock.patch.object(MetadataManager, "create", return_value=None),
                mock.patch.object(MetadataManager, "update", return_value=None),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.columns == [
            "attribute",
            "appealReference",
            "TEMP_PK",
            "ODTSourceSystem",
            "IngestionDate",
            "IsActive",
        ]
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 2
