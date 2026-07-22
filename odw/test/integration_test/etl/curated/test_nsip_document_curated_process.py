import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_document_curated_process import (
    NsipDocumentCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
import mock


class TestNSIPDocumntCurated(ETLTestCase):
    def test__nsip_document_curated_process__run__applies_expected_transformations(
        self,
    ):
        test_case = "t_ndcp_r_aet"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_docs = spark.createDataFrame(
            [
                (
                    1,
                    100,
                    "EN010001",
                    "DOC-REF-1",
                    1,
                    "EX-1",
                    "file1.pdf",
                    "original1.pdf",
                    123,
                    "application/pdf",
                    None,
                    "http://published/1",
                    "/docs/1",
                    "clean",
                    "md5-1",
                    "2025-01-01",
                    "2025-01-02",
                    "NSIP",
                    "No",
                    "Depublished",
                    "2025-01-03",
                    "Application Form",
                    "OFFICIAL",
                    "Horizon",
                    "origin-1",
                    "owner-1",
                    "author-1",
                    "author-cy-1",
                    "rep-1",
                    "description-1",
                    "description-cy-1",
                    "Developer's Application",
                    "filter-1",
                    "filter-cy-1",
                    "filter2-1",
                    "folder-1",
                    "transcript-1",
                    "Y",
                ),
                (
                    2,
                    200,
                    "EN010002",
                    "DOC-REF-2",
                    2,
                    "EX-2",
                    "file2.pdf",
                    "original2.pdf",
                    456,
                    "application/pdf",
                    "http://doc/2",
                    "http://published/2",
                    "/docs/2",
                    "clean",
                    "md5-2",
                    "2025-02-01",
                    "2025-02-02",
                    "Appeals",
                    "Yes",
                    "Ready To Publish",
                    "2025-02-03",
                    "Report",
                    "OFFICIAL-SENSITIVE",
                    "ODT",
                    "origin-2",
                    "owner-2",
                    "author-2",
                    "author-cy-2",
                    "rep-2",
                    "description-2",
                    "description-cy-2",
                    "Post decision",
                    "filter-2",
                    "filter-cy-2",
                    "filter2-2",
                    "folder-2",
                    "transcript-2",
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("documentId", T.IntegerType(), True),
                    T.StructField("caseId", T.IntegerType(), True),
                    T.StructField("caseRef", T.StringType(), True),
                    T.StructField("documentReference", T.StringType(), True),
                    T.StructField("version", T.IntegerType(), True),
                    T.StructField("examinationRefNo", T.StringType(), True),
                    T.StructField("filename", T.StringType(), True),
                    T.StructField("originalFilename", T.StringType(), True),
                    T.StructField("size", T.IntegerType(), True),
                    T.StructField("mime", T.StringType(), True),
                    T.StructField("documentURI", T.StringType(), True),
                    T.StructField("publishedDocumentURI", T.StringType(), True),
                    T.StructField("path", T.StringType(), True),
                    T.StructField("virusCheckStatus", T.StringType(), True),
                    T.StructField("fileMD5", T.StringType(), True),
                    T.StructField("dateCreated", T.StringType(), True),
                    T.StructField("lastModified", T.StringType(), True),
                    T.StructField("caseType", T.StringType(), True),
                    T.StructField("redactedStatus", T.StringType(), True),
                    T.StructField("PublishedStatus", T.StringType(), True),
                    T.StructField("datePublished", T.StringType(), True),
                    T.StructField("documentType", T.StringType(), True),
                    T.StructField("securityClassification", T.StringType(), True),
                    T.StructField("sourceSystem", T.StringType(), True),
                    T.StructField("origin", T.StringType(), True),
                    T.StructField("owner", T.StringType(), True),
                    T.StructField("author", T.StringType(), True),
                    T.StructField("authorWelsh", T.StringType(), True),
                    T.StructField("representative", T.StringType(), True),
                    T.StructField("description", T.StringType(), True),
                    T.StructField("descriptionWelsh", T.StringType(), True),
                    T.StructField("documentCaseStage", T.StringType(), True),
                    T.StructField("filter1", T.StringType(), True),
                    T.StructField("filter1Welsh", T.StringType(), True),
                    T.StructField("filter2", T.StringType(), True),
                    T.StructField("horizonFolderId", T.StringType(), True),
                    T.StructField("transcriptId", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        curated_projects = spark.createDataFrame(
            [("EN010001",), ("EN010999",)],
            T.StructType([T.StructField("caseReference", T.StringType(), True)]),
        )

        harmonised_table = f"{test_case}_nsip_document"
        self.write_existing_table(
            spark,
            harmonised_docs,
            harmonised_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_table,
            "overwrite",
        )
        curated_projects_table = f"{test_case}_nsip_project"
        self.write_existing_table(
            spark,
            curated_projects,
            curated_projects_table,
            "odw_curated_db",
            "odw-curated",
            curated_projects_table,
            "overwrite",
        )
        curated_nsip_document_table = f"{test_case}_nsip_document"

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_document_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipDocumentCuratedProcess,
                "HARMONISED_TABLE",
                f"odw_harmonised_db.{harmonised_table}",
            ),
            mock.patch.object(
                NsipDocumentCuratedProcess,
                "CURATED_PROJECT_TABLE",
                f"odw_curated_db.{curated_projects_table}",
            ),
            mock.patch.object(
                NsipDocumentCuratedProcess, "OUTPUT_TABLE", curated_nsip_document_table
            ),
        ):
            inst = NsipDocumentCuratedProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_document",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{curated_nsip_document_table}")
        rows = {
            row["documentId"]: row.asDict(recursive=True) for row in actual_df.collect()
        }

        assert actual_df.count() == 2

        assert rows[1]["documentURI"] == ""
        assert rows[1]["caseType"] == "nsip"
        assert rows[1]["publishedStatus"] == "unpublished"
        assert rows[1]["documentCaseStage"] == "developers_application"

        assert rows[2]["documentURI"] == "http://doc/2"
        assert rows[2]["caseType"] == "appeals"
        assert rows[2]["publishedStatus"] == "ready_to_publish"
        assert rows[2]["documentCaseStage"] == "post_decision"

        assert result.metadata.insert_count == 2
