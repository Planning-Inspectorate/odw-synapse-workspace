import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_document_curated_process import AppealDocumentCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def _harmonised_schema():
    return StructType(
        [
            StructField("AppealsDocumentMetadataID", LongType(), True),
            StructField("documentId", StringType(), True),
            StructField("caseId", IntegerType(), True),
            StructField("caseReference", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("filename", StringType(), True),
            StructField("originalFilename", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("mime", StringType(), True),
            StructField("documentURI", StringType(), True),
            StructField("publishedDocumentURI", StringType(), True),
            StructField("virusCheckStatus", StringType(), True),
            StructField("fileMD5", StringType(), True),
            StructField("dateCreated", StringType(), True),
            StructField("dateReceived", StringType(), True),
            StructField("datePublished", StringType(), True),
            StructField("lastModified", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("redactedStatus", StringType(), True),
            StructField("documentType", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("author", StringType(), True),
            StructField("description", StringType(), True),
            StructField("caseStage", StringType(), True),
            StructField("horizonFolderId", StringType(), True),
            StructField("caseNumber", StringType(), True),
            StructField("caseworkTypeGroup", StringType(), True),
            StructField("caseworkTypeAbbreviation", StringType(), True),
            StructField("versionFilename", StringType(), True),
            StructField("incomingOutgoingExternal", StringType(), True),
            StructField("publishedStatus", StringType(), True),
            StructField("Migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _curated_schema():
    return StructType(
        [
            StructField("documentId", StringType(), True),
            StructField("caseId", IntegerType(), True),
            StructField("caseReference", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("filename", StringType(), True),
            StructField("originalFilename", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("mime", StringType(), True),
            StructField("documentURI", StringType(), True),
            StructField("publishedDocumentURI", StringType(), True),
            StructField("virusCheckStatus", StringType(), True),
            StructField("fileMD5", StringType(), True),
            StructField("dateCreated", StringType(), True),
            StructField("dateReceived", StringType(), True),
            StructField("datePublished", StringType(), True),
            StructField("lastModified", StringType(), True),
            StructField("caseType", StringType(), True),
            StructField("redactedStatus", StringType(), True),
            StructField("documentType", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("author", StringType(), True),
            StructField("description", StringType(), True),
            StructField("caseStage", StringType(), True),
            StructField("horizonFolderId", StringType(), True),
        ]
    )


def _harmonised_row(**overrides):
    row = {
        "AppealsDocumentMetadataID": 1,
        "documentId": "doc-001",
        "caseId": 1001,
        "caseReference": "APP/001",
        "version": 1,
        "filename": "decision-letter.pdf",
        "originalFilename": "decision-letter-original.pdf",
        "size": 12345,
        "mime": "application/pdf",
        "documentURI": "https://example/doc-001/v1",
        "publishedDocumentURI": "https://example/public/doc-001/v1",
        "virusCheckStatus": "Clean",
        "fileMD5": "abc123",
        "dateCreated": "2025-01-01T09:00:00",
        "dateReceived": "2025-01-01T10:00:00",
        "datePublished": "2025-01-02T10:00:00",
        "lastModified": "2025-01-03T10:00:00",
        "caseType": "Householder (HAS) Appeal",
        "redactedStatus": "No",
        "documentType": "Decision",
        "sourceSystem": "Appeals",
        "origin": "Portal",
        "owner": "Case Officer",
        "author": "Inspector",
        "description": "Decision letter",
        "caseStage": "Decision",
        "horizonFolderId": "F-001",
        "caseNumber": None,
        "caseworkTypeGroup": None,
        "caseworkTypeAbbreviation": None,
        "versionFilename": None,
        "incomingOutgoingExternal": None,
        "publishedStatus": None,
        "Migrated": "0",
        "ODTSourceSystem": "ODT",
        "IngestionDate": "2025-01-10T10:00:00.000000+0000",
        "ValidTo": None,
        "RowID": "row-id",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


def _curated_row(**overrides):
    row = {
        "documentId": "doc-001",
        "caseId": 1001,
        "caseReference": "APP/001",
        "version": 1,
        "filename": "decision-letter.pdf",
        "originalFilename": "decision-letter-original.pdf",
        "size": 12345,
        "mime": "application/pdf",
        "documentURI": "https://example/doc-001/v1",
        "publishedDocumentURI": "https://example/public/doc-001/v1",
        "virusCheckStatus": "Clean",
        "fileMD5": "abc123",
        "dateCreated": "2025-01-01T09:00:00",
        "dateReceived": "2025-01-01T10:00:00",
        "datePublished": "2025-01-02T10:00:00",
        "lastModified": "2025-01-03T10:00:00",
        "caseType": "D",
        "redactedStatus": "No",
        "documentType": "Decision",
        "sourceSystem": "Appeals",
        "origin": "Portal",
        "owner": "Case Officer",
        "author": "Inspector",
        "description": "Decision letter",
        "caseStage": "Decision",
        "horizonFolderId": "F-001",
    }
    row.update(overrides)
    return row


class TestAppealDocumentCuratedProcess(ETLTestCase):
    def compare_curated_data(self, expected_df: DataFrame, actual_df: DataFrame):
        assert_dataframes_equal(expected_df, actual_df)

    def write_source_table(self, spark, table_df: DataFrame):
        self.write_existing_table(
            spark,
            table_df,
            "appeal_document",
            "odw_harmonised_db",
            "odw-harmonised",
            "appeal_document",
            "overwrite",
        )

    def write_target_table(self, spark, table_df: DataFrame):
        self.write_existing_table(
            spark,
            table_df,
            "appeal_document",
            "odw_curated_db",
            "odw-curated",
            "appeal_document",
            "overwrite",
        )

    def write_empty_target_table(self, spark):
        empty_target_df = spark.createDataFrame([], _curated_schema())
        self.write_target_table(spark, empty_target_df)

    def test__appeal_document_curated_process__run__initial_load_matches_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                documentId="doc-001",
                caseType="Householder (HAS) Appeal",
                IsActive="Y",
            ),
            _harmonised_row(
                documentId="doc-002",
                caseId=1002,
                caseReference="APP/002",
                filename="statement.pdf",
                originalFilename="statement.pdf",
                caseType="Planning Appeal (A)",
                IsActive="Y",
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(
                    documentId="doc-001",
                    caseType="D",
                ),
                _curated_row(
                    documentId="doc-002",
                    caseId=1002,
                    caseReference="APP/002",
                    filename="statement.pdf",
                    originalFilename="statement.pdf",
                    caseType="W",
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__filters_out_inactive_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(documentId="doc-active", IsActive="Y"),
            _harmonised_row(documentId="doc-inactive", IsActive="N"),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(documentId="doc-active"),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__coalesces_mime_and_documenturi_to_empty_string(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                documentId="doc-null-fields",
                mime=None,
                documentURI=None,
                IsActive="Y",
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(
                    documentId="doc-null-fields",
                    mime="",
                    documentURI="",
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__preserves_null_publisheddocumenturi(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                documentId="doc-null-published-uri",
                publishedDocumentURI=None,
                IsActive="Y",
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(
                    documentId="doc-null-published-uri",
                    publishedDocumentURI=None,
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__deduplicates_distinct_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate_row = _harmonised_row(documentId="doc-dup", IsActive="Y")
        source_rows = [duplicate_row, duplicate_row]

        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(documentId="doc-dup"),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__unmapped_casetype_is_preserved(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                documentId="doc-unmapped-case",
                caseType="Some Unmapped Case Type",
                IsActive="Y",
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(
                    documentId="doc-unmapped-case",
                    caseType="Some Unmapped Case Type",
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__appeal_document_curated_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        self.write_source_table(spark, spark.createDataFrame([], _harmonised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame([], _curated_schema())

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"
            ) as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = AppealDocumentCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.appeal_document")
        self.compare_curated_data(expected_table_data, actual_table_data)