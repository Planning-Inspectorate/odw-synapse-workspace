from datetime import datetime
import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T
from odw.core.etl.transformation.harmonised.aie_document_harmonisation_process import AieDocumentHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful, assert_dataframes_equal


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
        T.StructField("IngestionDate", T.TimestampType(), True),
        T.StructField("ValidTo", T.StringType(), True),
        T.StructField("RowID", T.StringType(), True),
        T.StructField("IsActive", T.StringType(), True),
        T.StructField("expected_from", T.TimestampType(), True),
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
        datetime(2024, 1, 1),
    )


def _harmonise_schema():
    return T.StructType(
        [
            T.StructField("AIEDocumentDataID", T.IntegerType(), True),
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
            T.StructField("IngestionDate", T.TimestampType(), True),
            T.StructField("ValidTo", T.StringType(), True),
            T.StructField("RowID", T.StringType(), True),
            T.StructField("IsActive", T.StringType(), True),
        ]
    )


def _harmonised_row(**overrides):
    base = {
        "AIEDocumentDataID": 2,
        "documentId": "1",
        "caseRef": "REF001",
        "documentReference": "DOC-REF",
        "version": "1",
        "examinationRefNo": "EXAM-1",
        "filename": "file1.pdf",
        "originalFilename": "file1.pdf",
        "size": "1000",
        "mime": "application/pdf",
        "documentUri": "http://example.com/doc",
        "path": "/path/doc",
        "virusCheckStatus": "clean",
        "fileMD5": "new-md5",
        "dateCreated": "2024-01-01",
        "lastModified": "2024-01-02",
        "caseType": "typeA",
        "documentStatus": "published",
        "redactedStatus": "not_redacted",
        "publishedStatus": "published",
        "datePublished": "2024-01-01",
        "documentType": "docType",
        "securityClassification": "public",
        "sourceSystem": "Horizon",
        "origin": "origin1",
        "owner": "owner1",
        "author": "author1",
        "representative": "rep1",
        "description": "description",
        "stage": "stage1",
        "filter1": "f1",
        "filter2": "f2",
        "Migrated": "0",
        "ODTSourceSystem": "Horizon",
        "IngestionDate": datetime(2024, 1, 1, 0, 0),
        "ValidTo": "2024-01-01 00:00:00",
        "RowID": "ecf8161eb0a8c6036c9c2743943d5f3d",
        "IsActive": "N",
    }
    return base | overrides


class TestAieDocumentHarmonisationProcessIntegration(ETLTestCase):
    def test__aie_document_harmonisation_process__run__single_record_writes_active_row_with_expected_metadata(self):
        test_case = "t_adhp_r_srwarwem"
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame(
            [_horizon_row("pk1", "1", "file1.pdf", "1", datetime(2024, 1, 1))],
            _HORIZON_SCHEMA,
        )
        horizon_data_table = f"{test_case}_aie_document_data"
        self.write_existing_table(spark, horizon_data, horizon_data_table, "odw_standardised_db", "odw-standardised", horizon_data_table, "overwrite")
        aie_table = f"{test_case}_aie_document_data"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(AieDocumentHarmonisationProcess, "HORIZON_TABLE", f"odw_standardised_db.{horizon_data_table}"),
            mock.patch.object(AieDocumentHarmonisationProcess, "OUTPUT_TABLE", aie_table),
        ):
            inst = AieDocumentHarmonisationProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="aie_document", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)

        df = spark.table(f"odw_harmonised_db.{aie_table}")
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["AIEDocumentDataID"] == 1
        assert rows[0]["RowID"] is not None and len(rows[0]["RowID"]) == 32
        assert "TEMP_PK" not in df.columns

    def test__aie_document_harmonisation_process__run__same_pk_across_ingestion_dates_produces_scd2_history(self):
        test_case = "t_adhp_r_spaidpsh"
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame(
            [
                _horizon_row("pk1", "1", "file1.pdf", "1", datetime(2024, 1, 1), file_md5="old-md5"),
                _horizon_row("pk1", "1", "file1.pdf", "1", datetime(2024, 2, 1), file_md5="new-md5"),
            ],
            _HORIZON_SCHEMA,
        )
        horizon_data_table = f"{test_case}_aie_document_data"
        self.write_existing_table(spark, horizon_data, horizon_data_table, "odw_standardised_db", "odw-standardised", horizon_data_table, "overwrite")
        aie_table = f"{test_case}_aie_document_data"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(AieDocumentHarmonisationProcess, "HORIZON_TABLE", f"odw_standardised_db.{horizon_data_table}"),
            mock.patch.object(AieDocumentHarmonisationProcess, "OUTPUT_TABLE", aie_table),
        ):
            inst = AieDocumentHarmonisationProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="aie_document", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)

        expected_df = spark.createDataFrame(
            (
                _harmonised_row(),
                _harmonised_row(fileMD5="old-md5", RowID="2f5731906a6583d9870434e1214b1ab8"),
                _harmonised_row(AIEDocumentDataID=1, ValidTo=None, IsActive="Y"),
                _harmonised_row(AIEDocumentDataID=1, fileMD5="old-md5", ValidTo=None, RowID="2f5731906a6583d9870434e1214b1ab8", IsActive="Y"),
            ),
            _harmonise_schema(),
        )

        df = spark.table(f"odw_harmonised_db.{aie_table}")
        assert_dataframes_equal(expected_df, df)

    def test__aie_document_harmonisation_process__run__empty_source_writes_empty_output(self):
        test_case = "t_adhp_r_esweo"
        spark = PytestSparkSessionUtil().get_spark_session()

        horizon_data = spark.createDataFrame([], _HORIZON_SCHEMA)
        horizon_data_table = f"{test_case}_aie_document_data"
        self.write_existing_table(spark, horizon_data, horizon_data_table, "odw_standardised_db", "odw-standardised", horizon_data_table, "overwrite")
        aie_table = f"{test_case}_aie_document_data"

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(AieDocumentHarmonisationProcess, "HORIZON_TABLE", f"odw_standardised_db.{horizon_data_table}"),
            mock.patch.object(AieDocumentHarmonisationProcess, "OUTPUT_TABLE", aie_table),
        ):
            inst = AieDocumentHarmonisationProcess(spark)

            result = inst.run(orchestration_run_id=test_case, orchestration_entity_name="aie_document", orchestration_stage_name="harmonise")
            assert_etl_result_successful(result)

        df = spark.table(f"odw_harmonised_db.{aie_table}")

        assert df.count() == 0
