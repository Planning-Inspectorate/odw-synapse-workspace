from odw.core.etl.transformation.curated.nsip_document_curated_process import NsipDocumentCuratedProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


class TestNSIPDocumentCurationProcess(SparkTestCase):
    def test__nsip_document_curated_process__process__applies_expected_transformations(self):
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
                ]
            ),
        )

        curated_projects = spark.createDataFrame(
            [("EN010001",), ("EN010999",)],
            T.StructType([T.StructField("caseReference", T.StringType(), True)]),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_document_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.transformation.curated.nsip_document_curated_process.LoggingUtil"),
        ):
            inst = NsipDocumentCuratedProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "harmonised_docs": harmonised_docs,
                    "curated_projects": curated_projects,
                }
            )

        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = {row["documentId"]: row.asDict(recursive=True) for row in actual_df.collect()}

        assert actual_df.count() == 2

        assert rows[1]["documentURI"] == ""
        assert rows[1]["caseType"] == "nsip"
        assert rows[1]["publishedStatus"] == "unpublished"
        assert rows[1]["documentCaseStage"] == "developers_application"

        assert rows[2]["documentURI"] == "http://doc/2"
        assert rows[2]["caseType"] == "appeals"
        assert rows[2]["publishedStatus"] == "ready_to_publish"
        assert rows[2]["documentCaseStage"] == "post_decision"

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_document"
        assert result.metadata.insert_count == 2
