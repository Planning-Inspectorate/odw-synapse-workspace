from odw.core.etl.transformation.harmonised.nsip_document_harmonisation_process import NsipDocumentHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


def test__nsip_document_harmonisation_process__process__combines_service_bus_and_horizon_and_sets_migrated_flags():
    spark = PytestSparkSessionUtil().get_spark_session()

    service_bus_data = spark.createDataFrame(
        [
            (
                "pk1",
                1,
                10,
                100,
                "EN010001",
                "DOCREF1",
                1,
                "EX1",
                "file1.pdf",
                "original1.pdf",
                100,
                "application/pdf",
                "http://doc1",
                "http://pub1",
                "/path1",
                "clean",
                "md5-1",
                "2025-01-01",
                "2025-01-02",
                "NSIP",
                "No",
                "Published",
                "2025-01-03",
                "Type1",
                "OFFICIAL",
                "ODT",
                "origin1",
                "owner1",
                "author1",
                "authorcy1",
                "rep1",
                "desc1",
                "desccy1",
                "Stage1",
                "f1",
                "f1cy",
                "f2",
                "folder1",
                "transcript1",
                "1",
                "ODT",
                "SRC1",
                "2025-01-10 00:00:00",
                None,
                "",
                "Y",
            )
        ],
        T.StructType(
            [
                T.StructField("TEMP_PK", T.StringType(), True),
                T.StructField("NSIPDocumentID", T.IntegerType(), True),
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
                T.StructField("publishedStatus", T.StringType(), True),
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
                T.StructField("Migrated", T.StringType(), True),
                T.StructField("ODTSourceSystem", T.StringType(), True),
                T.StructField("SourceSystemID", T.StringType(), True),
                T.StructField("IngestionDate", T.StringType(), True),
                T.StructField("ValidTo", T.StringType(), True),
                T.StructField("RowID", T.StringType(), True),
                T.StructField("IsActive", T.StringType(), True),
            ]
        ),
    )

    horizon_data = spark.createDataFrame(
        [
            (20, 200, "EN010002", "DOCREF2", 1, "file2.pdf", "original2.pdf", 120, "clean", "2025-02-01", "2025-02-02", "NSIP", "Published", "2025-02-03", "Type2", "Horizon", "author2", "authorcy2", "rep2", "desc2", "desccy2", "Stage2", "f21", "f21cy", "f22", "folder2", "2025-02-10 00:00:00"),
        ],
        [
            "dataId",
            "casenodeid",
            "caseReference",
            "documentReference",
            "version",
            "name",
            "originalFilename",
            "dataSize",
            "virusCheckStatus",
            "createDate",
            "modifyDate",
            "caseworkType",
            "publishedStatus",
            "datePublished",
            "documentType",
            "sourceSystem",
            "author",
            "authorWelsh",
            "representative",
            "documentDescription",
            "documentDescriptionWelsh",
            "documentCaseStage",
            "filter1",
            "filter1Welsh",
            "filter2",
            "parentid",
            "expected_from",
        ],
    )

    aie_data = spark.createDataFrame(
        [
            (20, 1, 120, "EX2", "application/pdf", "http://doc2", "/path2", "md5-2", "OFFICIAL", "origin2", "owner2"),
        ],
        [
            "DocumentId",
            "version",
            "size",
            "examinationRefNo",
            "mime",
            "documentURI",
            "path",
            "fileMD5",
            "securityClassification",
            "origin",
            "owner",
        ],
    )

    sb_primary_keys = spark.createDataFrame(
        [("pk1",)],
        T.StructType([T.StructField("TEMP_PK", T.StringType(), True)]),
    )

    with mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_document_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
    ), mock.patch(
        "odw.core.etl.transformation.harmonised.nsip_document_harmonisation_process.LoggingUtil"
    ):
        inst = NsipDocumentHarmonisationProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "service_bus_data": service_bus_data,
                "horizon_data": horizon_data,
                "aie_data": aie_data,
                "sb_primary_keys": sb_primary_keys,
            }
        )

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = {row["documentId"]: row.asDict(recursive=True) for row in actual_df.collect()}

    assert actual_df.count() == 2
    assert rows[10]["Migrated"] == "1"
    assert rows[20]["Migrated"] == "0"
    assert rows[10]["IsActive"] == "Y"
    assert rows[20]["IsActive"] == "Y"

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert data_to_write[inst.OUTPUT_TABLE]["partition_by"] == ["IsActive"]
    assert result.metadata.insert_count == 2