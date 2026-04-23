import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_document_curated_process import AppealDocumentCuratedProcess
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def _harmonised_schema():
    return StructType(
        [
            StructField("AppealsDocumentMetadataID", IntegerType(), True),
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


class TestAppealDocumentCuratedProcess(SparkTestCase):
    def test__appeal_document_curated_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = AppealDocumentCuratedProcess(spark)

        assert inst.get_name() == "appeal_document_curated_process"

    def test__appeal_document_curated_process__process__filters_to_active_rows_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(documentId="doc-active", IsActive="Y"),
            _harmonised_row(documentId="doc-inactive", IsActive="N"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("documentId") == "doc-active").count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_document_curated_process__process__maps_known_casetype_values_to_legacy_codes(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(documentId="doc-1", caseType="Householder (HAS) Appeal", IsActive="Y"),
            _harmonised_row(documentId="doc-2", caseType="Planning Appeal (A)", IsActive="Y"),
            _harmonised_row(documentId="doc-3", caseType="Lawful Development Certificate Appeal", IsActive="Y"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"].select("documentId", "caseType").orderBy("documentId")

        expected_df = spark.createDataFrame(
            [
                ("doc-1", "D"),
                ("doc-2", "W"),
                ("doc-3", "X"),
            ],
            ["documentId", "caseType"],
        ).orderBy("documentId")

        assert_dataframes_equal(expected_df, actual_df)

    def test__appeal_document_curated_process__process__preserves_unmapped_casetype(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                documentId="doc-unmapped",
                caseType="Some Unmapped Case Type",
                IsActive="Y",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["caseType"] == "Some Unmapped Case Type"

    def test__appeal_document_curated_process__process__coalesces_mime_and_documenturi_to_empty_string(
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["mime"] == ""
        assert row["documentURI"] == ""

    def test__appeal_document_curated_process__process__does_not_coalesce_publisheddocumenturi(
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["publishedDocumentURI"] is None

    def test__appeal_document_curated_process__process__selects_expected_columns_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame([_harmonised_row()], schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "documentId",
            "caseId",
            "caseReference",
            "version",
            "filename",
            "originalFilename",
            "size",
            "mime",
            "documentURI",
            "publishedDocumentURI",
            "virusCheckStatus",
            "fileMD5",
            "dateCreated",
            "dateReceived",
            "datePublished",
            "lastModified",
            "caseType",
            "redactedStatus",
            "documentType",
            "sourceSystem",
            "origin",
            "owner",
            "author",
            "description",
            "caseStage",
            "horizonFolderId",
        ]

    def test__appeal_document_curated_process__process__deduplicates_identical_rows_via_distinct(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate_row = _harmonised_row(documentId="doc-dup", IsActive="Y")

        source_data = {
            "source_data": spark.createDataFrame([duplicate_row, duplicate_row], schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_document_curated_process__process__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_harmonised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.curated.appeal_document_curated_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.curated.appeal_document_curated_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
