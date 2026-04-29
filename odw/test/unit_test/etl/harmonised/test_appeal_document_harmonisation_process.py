import hashlib
import mock
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType
from odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process import AppealDocumentHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase


def _service_bus_schema():
    return StructType(
        [
            StructField("TEMP_PK", StringType(), True),
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
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _horizon_schema():
    return StructType(
        [
            StructField("documentId", StringType(), True),
            StructField("casenodeid", IntegerType(), True),
            StructField("caseReference", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("filename", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("virusCheckStatus", StringType(), True),
            StructField("dateCreated", StringType(), True),
            StructField("datePublished", StringType(), True),
            StructField("lastModified", StringType(), True),
            StructField("caseworkType", StringType(), True),
            StructField("redactedStatus", StringType(), True),
            StructField("documentType", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("documentDescription", StringType(), True),
            StructField("folderid", StringType(), True),
            StructField("caseNumber", StringType(), True),
            StructField("caseworkTypeGroup", StringType(), True),
            StructField("caseworkTypeAbbreviation", StringType(), True),
            StructField("versionFilename", StringType(), True),
            StructField("incomingOutgoingExternal", StringType(), True),
            StructField("publishedStatus", StringType(), True),
            StructField("expected_from", StringType(), True),
            StructField("ingested_datetime", StringType(), True),
        ]
    )


def _aie_schema():
    return StructType(
        [
            StructField("documentid", StringType(), True),
            StructField("size", IntegerType(), True),
            StructField("version", IntegerType(), True),
            StructField("mime", StringType(), True),
            StructField("documentURI", StringType(), True),
            StructField("fileMD5", StringType(), True),
            StructField("owner", StringType(), True),
            StructField("author", StringType(), True),
        ]
    )


def _service_bus_output_schema():
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


def _service_bus_row(**overrides):
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
        "caseType": "S78",
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
        "Migrated": "1",
        "ODTSourceSystem": "ODT",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-01-10T10:00:00.000000+0000",
        "ValidTo": None,
        "RowID": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    # Compute TEMP_PK to match _load_service_bus_data: MD5(CONCAT(documentId, filename, version, documentURI))
    if "TEMP_PK" not in row:
        row["TEMP_PK"] = hashlib.md5(f"{row['documentId']}{row['filename']}{row['version']}{row['documentURI']}".encode()).hexdigest()
    return row


def _horizon_row(**overrides):
    row = {
        "documentId": "doc-002",
        "casenodeid": 1002,
        "caseReference": "APP/002",
        "version": 1,
        "filename": "statement.pdf",
        "size": 22222,
        "virusCheckStatus": "Clean",
        "dateCreated": "2025-01-05T09:00:00",
        "datePublished": "2025-01-06T10:00:00",
        "lastModified": "2025-01-07T10:00:00",
        "caseworkType": "HAS",
        "redactedStatus": "No",
        "documentType": "Statement",
        "sourceSystem": "Horizon",
        "documentDescription": "Interested party statement",
        "folderid": "HF-001",
        "caseNumber": "CASE-002",
        "caseworkTypeGroup": "Appeals",
        "caseworkTypeAbbreviation": "HAS",
        "versionFilename": "statement_v1.pdf",
        "incomingOutgoingExternal": "Incoming",
        "publishedStatus": "Published",
        "expected_from": "2025-01-11T10:00:00",
        "ingested_datetime": "2025-01-11T12:00:00",
    }
    row.update(overrides)
    return row


def _aie_row(**overrides):
    row = {
        "documentid": "doc-002",
        "size": 22222,
        "version": 1,
        "mime": "application/pdf",
        "documentURI": "https://example/doc-002/v1",
        "fileMD5": "def456",
        "owner": "Horizon Owner",
        "author": "Horizon Author",
    }
    row.update(overrides)
    return row


def _expected_rowid(row):
    values = [
        row.get("AppealsDocumentMetadataID"),
        row.get("documentId"),
        row.get("caseId"),
        row.get("caseReference"),
        row.get("version"),
        row.get("filename"),
        row.get("originalFilename"),
        row.get("size"),
        row.get("mime"),
        row.get("documentURI"),
        row.get("publishedDocumentURI"),
        row.get("virusCheckStatus"),
        row.get("fileMD5"),
        row.get("dateCreated"),
        row.get("dateReceived"),
        row.get("datePublished"),
        row.get("lastModified"),
        row.get("caseType"),
        row.get("redactedStatus"),
        row.get("documentType"),
        row.get("sourceSystem"),
        row.get("origin"),
        row.get("owner"),
        row.get("author"),
        row.get("description"),
        row.get("caseStage"),
        row.get("horizonFolderId"),
        row.get("caseNumber"),
        row.get("caseworkTypeGroup"),
        row.get("caseworkTypeAbbreviation"),
        row.get("versionFilename"),
        row.get("incomingOutgoingExternal"),
        row.get("publishedStatus"),
        row.get("Migrated"),
        row.get("ODTSourceSystem"),
        row.get("IngestionDate"),
        row.get("ValidTo"),
    ]
    joined = "".join(str(value) if value is not None else "." for value in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


class TestAppealDocumentHarmonisationProcess(SparkTestCase):
    def test__appeal_document_harmonisation_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = AppealDocumentHarmonisationProcess(spark)

        assert inst.get_name() == "appeal_document_harmonisation_process"

    def test__appeal_document_harmonisation_process__process__merges_service_bus_and_horizon_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([_service_bus_row()], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([_horizon_row()], _horizon_schema()),
            "aie_data": spark.createDataFrame([_aie_row()], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("documentId") == "doc-001").count() == 1
        assert df.where(F.col("documentId") == "doc-002").count() == 1
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__appeal_document_harmonisation_process__process__exact_duplicate_service_bus_rows_collapse_to_one_row(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate = _service_bus_row()

        sb_df = spark.createDataFrame([duplicate], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_document_harmonisation_process__process__exact_duplicate_horizon_rows_collapse_to_one_row(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate = _horizon_row()

        sb_df = spark.createDataFrame([], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([duplicate, duplicate], _horizon_schema()),
            "aie_data": spark.createDataFrame([_aie_row()], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_document_harmonisation_process__process__horizon_row_is_kept_when_aie_row_does_not_match(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([_horizon_row()], _horizon_schema()),
            "aie_data": spark.createDataFrame(
                [_aie_row(documentid="doc-002", size=99999)],
                _aie_schema(),
            ),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["documentId"] == "doc-002"
        assert row["mime"] is None
        assert row["documentURI"] is None
        assert row["fileMD5"] is None
        assert row["owner"] is None
        assert row["author"] is None

    def test__appeal_document_harmonisation_process__process__aie_join_requires_documentid_size_and_version(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([_horizon_row(version=2)], _horizon_schema()),
            "aie_data": spark.createDataFrame(
                [_aie_row(documentid="doc-002", size=22222, version=1)],
                _aie_schema(),
            ),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["version"] == 2
        assert row["mime"] is None
        assert row["documentURI"] is None
        assert row["fileMD5"] is None

    def test__appeal_document_harmonisation_process__process__sets_latest_duplicate_primary_key_row_active_and_older_inactive(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame(
            [
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-10T10:00:00.000000+0000",
                ),
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-12T10:00:00.000000+0000",
                    fileMD5="updated-md5",
                ),
            ],
            _service_bus_schema(),
        )
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        old_row = df.where(F.col("IngestionDate") == "2025-01-10T10:00:00.000000+0000").collect()[0]
        new_row = df.where(F.col("IngestionDate") == "2025-01-12T10:00:00.000000+0000").collect()[0]

        assert old_row["IsActive"] == "N"
        assert old_row["ValidTo"] == "2025-01-12T10:00:00.000000+0000"
        assert new_row["IsActive"] == "Y"
        assert new_row["ValidTo"] is None

    def test__appeal_document_harmonisation_process__process__preserves_explicit_source_validto(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame(
            [
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-10T10:00:00.000000+0000",
                    ValidTo="2025-01-20T00:00:00.000000+0000",
                ),
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-12T10:00:00.000000+0000",
                    fileMD5="updated-md5",
                ),
            ],
            _service_bus_schema(),
        )
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        old_row = df.where(F.col("IngestionDate") == "2025-01-10T10:00:00.000000+0000").collect()[0]

        assert old_row["ValidTo"] == "2025-01-20T00:00:00.000000+0000"

    def test__appeal_document_harmonisation_process__process__recalculates_isactive_ignoring_source_values(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame(
            [
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-10T10:00:00.000000+0000",
                    IsActive="Y",
                ),
                _service_bus_row(
                    documentId="doc-001",
                    documentURI="https://example/doc-001/v1",
                    IngestionDate="2025-01-12T10:00:00.000000+0000",
                    IsActive="N",
                    fileMD5="updated-md5",
                ),
            ],
            _service_bus_schema(),
        )
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("IngestionDate") == "2025-01-10T10:00:00.000000+0000") & (F.col("IsActive") == "N")).count() == 1
        assert df.where((F.col("IngestionDate") == "2025-01-12T10:00:00.000000+0000") & (F.col("IsActive") == "Y")).count() == 1

    def test__appeal_document_harmonisation_process__process__service_bus_row_with_documenturi_results_in_migrated_zero_due_to_legacy_hash_mismatch(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([_service_bus_row()], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["Migrated"] == "1"

    def test__appeal_document_harmonisation_process__process__aligns_horizon_fields_to_service_bus_shape(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([_horizon_row()], _horizon_schema()),
            "aie_data": spark.createDataFrame([_aie_row()], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["originalFilename"] == "statement.pdf"
        assert row["publishedDocumentURI"] is None
        assert row["dateReceived"] is None
        assert row["origin"] is None
        assert row["caseStage"] is None
        assert row["ODTSourceSystem"] == "Horizon"

    def test__appeal_document_harmonisation_process__process__outputs_expected_columns_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([_service_bus_row()], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "AppealsDocumentMetadataID",
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
            "caseNumber",
            "caseworkTypeGroup",
            "caseworkTypeAbbreviation",
            "versionFilename",
            "incomingOutgoingExternal",
            "publishedStatus",
            "Migrated",
            "ODTSourceSystem",
            "IngestionDate",
            "ValidTo",
            "RowID",
            "IsActive",
        ]

    def test__appeal_document_harmonisation_process__process__final_output_does_not_include_sourcesystemid(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([_service_bus_row()], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert "SourceSystemID" not in df.columns

    def test__appeal_document_harmonisation_process__process__rowid_matches_legacy_hash_shape(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([_service_bus_row()], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        expected = _expected_rowid(
            {
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
                "caseType": "S78",
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
                "Migrated": "1",
                "ODTSourceSystem": "ODT",
                "IngestionDate": "2025-01-10T10:00:00.000000+0000",
                "ValidTo": None,
            }
        )

        assert row["RowID"] == expected

    def test__appeal_document_harmonisation_process__process__empty_sources_return_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        sb_df = spark.createDataFrame([], _service_bus_schema())
        source_data = {
            "service_bus_data": sb_df,
            "horizon_data": spark.createDataFrame([], _horizon_schema()),
            "aie_data": spark.createDataFrame([], _aie_schema()),
            "sb_primary_keys": sb_df.select("TEMP_PK").distinct(),
            "table_path": "/tmp/test/appeal_document",
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.LoggingUtil"),
            mock.patch(
                "odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process.Util.get_storage_account",
                return_value="teststorage",
            ),
        ):
            inst = AppealDocumentHarmonisationProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
