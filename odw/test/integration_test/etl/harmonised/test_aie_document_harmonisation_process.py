import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T
from odw.core.etl.transformation.harmonised.aie_document_harmonisation_process import AieDocumentHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


_HORIZON_SCHEMA = T.StructType(
    [
        T.StructField("TEMP_PK", T.StringType(), True),
        T.StructField("AIEDocumentDataID", T.LongType(), True),
        T.StructField("documentId", T.StringType(), True),
        T.StructField("caseRef", T.StringType(), True),
        T.StructField("documentReference", T.StringType(), True),
        T.StructField("version", T.StringType(), True),
        T.StructField("examinationRefNo", T.StringType(), True),
        T.StructField("filename", T.StringType(), True),
        T.StructField("originalFilename", T.StringType(), True),
        T.StructField("size", T.StringType(), True),
        T.StructField("mime", T.StringType(), True),
        T.StructField("documentUri", T.StringType(), True),
        T.StructField("path", T.StringType(), True),
        T.StructField("virusCheckStatus", T.StringType(), True),
        T.StructField("fileMD5", T.StringType(), True),
        T.StructField("dateCreated", T.StringType(), True),
        T.StructField("lastModified", T.StringType(), True),
        T.StructField("caseType", T.StringType(), True),
        T.StructField("documentStatus", T.StringType(), True),
        T.StructField("redactedStatus", T.StringType(), True),
        T.StructField("publishedStatus", T.StringType(), True),
        T.StructField("datePublished", T.StringType(), True),
        T.StructField("documentType", T.StringType(), True),
        T.StructField("securityClassification", T.StringType(), True),
        T.StructField("sourceSystem", T.StringType(), True),
        T.StructField("origin", T.StringType(), True),
        T.StructField("owner", T.StringType(), True),
        T.StructField("author", T.StringType(), True),
        T.StructField("representative", T.StringType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("stage", T.StringType(), True),
        T.StructField("filter1", T.StringType(), True),
        T.StructField("filter2", T.StringType(), True),
        T.StructField("Migrated", T.StringType(), True),
        T.StructField("ODTSourceSystem", T.StringType(), True),
        T.StructField("IngestionDate", T.StringType(), True),
        T.StructField("ValidTo", T.StringType(), True),
        T.StructField("RowID", T.StringType(), True),
        T.StructField("IsActive", T.StringType(), True),
    ]
)


def _horizon_row(temp_pk, document_id, filename, version, ingestion_date, file_md5="abc123"):
    """Build a row matching the schema produced by AieDocumentHarmonisationProcess._load_horizon_data()."""
    return (
        temp_pk,
        None,  # AIEDocumentDataID — always NULL out of load_data
        document_id,
        "REF001",
        "DOC-REF",
        version,
        "EXAM-1",
        filename,
        filename,
        "1000",
        "application/pdf",
        "http://example.com/doc",
        "/path/doc",
        "clean",
        file_md5,
        "2024-01-01",
        "2024-01-02",
        "typeA",
        "published",
        "not_redacted",
        "published",
        "2024-01-01",
        "docType",
        "public",
        "Horizon",
        "origin1",
        "owner1",
        "author1",
        "rep1",
        "description",
        "stage1",
        "f1",
        "f2",
        "0",
        "Horizon",
        ingestion_date,
        None,
        "",
        "Y",
    )


class TestAieDocumentHarmonisationProcessIntegration(ETLTestCase):
    def test__aie_document_harmonisation_process__run__single_record_writes_active_row_with_expected_metadata(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame(
            [_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        source_data = {"horizon_data": horizon_data}

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.LoggingUtil") as mock_proc_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = AieDocumentHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["AIEDocumentDataID"] == 1
        assert rows[0]["RowID"] is not None and len(rows[0]["RowID"]) == 32
        assert "TEMP_PK" not in df.columns

        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__aie_document_harmonisation_process__run__same_pk_across_ingestion_dates_produces_scd2_history(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame(
            [
                _horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00", file_md5="old-md5"),
                _horizon_row("pk1", "1", "file1.pdf", "1", "2024-02-01 00:00:00", file_md5="new-md5"),
            ],
            _HORIZON_SCHEMA,
        )
        source_data = {"horizon_data": horizon_data}

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.LoggingUtil") as mock_proc_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = AieDocumentHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        by_ingestion = {row["IngestionDate"]: row for row in df.collect()}

        assert df.count() == 2
        assert by_ingestion["2024-02-01 00:00:00"]["IsActive"] == "Y"
        assert by_ingestion["2024-02-01 00:00:00"]["ValidTo"] is None
        assert by_ingestion["2024-01-01 00:00:00"]["IsActive"] == "N"
        assert by_ingestion["2024-01-01 00:00:00"]["ValidTo"] == "2024-02-01 00:00:00"
        assert result.metadata.insert_count == 2

    def test__aie_document_harmonisation_process__run__empty_source_writes_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame([], _HORIZON_SCHEMA)
        source_data = {"horizon_data": horizon_data}

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.LoggingUtil") as mock_proc_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = AieDocumentHarmonisationProcess(spark)

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
