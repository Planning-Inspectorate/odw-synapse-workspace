from odw.core.etl.transformation.harmonised.nsip_s51_advice_harmonisation_process import NsipS51AdviceHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


def test__nsip_s51_advice_harmonisation_process__process__aggregates_attachments_and_applies_delete_logic():
    spark = PytestSparkSessionUtil().get_spark_session()

    service_bus_data = spark.createDataFrame(
        [
            (
                1,
                10,
                "ADV-10",
                100,
                "EN010001",
                "Title 10",
                None,
                "From 10",
                "Agent 10",
                "email",
                "2025-01-01",
                "Enquiry 10",
                None,
                "Advice By 10",
                "2025-01-02",
                "Advice 10",
                None,
                "Published",
                None,
                ["SBA1"],
                "Yes",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "1",
                "ODT",
                "SRC1",
                "2025-01-01 00:00:00",
                None,
                "",
                "Y",
            ),
        ],
        T.StructType(
            [
                T.StructField("NSIPAdviceID", T.IntegerType(), True),
                T.StructField("adviceId", T.IntegerType(), True),
                T.StructField("adviceReference", T.StringType(), True),
                T.StructField("caseId", T.IntegerType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("titleWelsh", T.StringType(), True),
                T.StructField("from", T.StringType(), True),
                T.StructField("agent", T.StringType(), True),
                T.StructField("method", T.StringType(), True),
                T.StructField("enquiryDate", T.StringType(), True),
                T.StructField("enquiryDetails", T.StringType(), True),
                T.StructField("enquiryDetailsWelsh", T.StringType(), True),
                T.StructField("adviceGivenBy", T.StringType(), True),
                T.StructField("adviceDate", T.StringType(), True),
                T.StructField("adviceDetails", T.StringType(), True),
                T.StructField("adviceDetailsWelsh", T.StringType(), True),
                T.StructField("status", T.StringType(), True),
                T.StructField("redactionStatus", T.StringType(), True),
                T.StructField("attachmentIds", T.ArrayType(T.StringType()), True),
                T.StructField("Section51Advice", T.StringType(), True),
                T.StructField("EnquirerFirstName", T.StringType(), True),
                T.StructField("EnquirerLastName", T.StringType(), True),
                T.StructField("AdviceLastModified", T.StringType(), True),
                T.StructField("AttachmentCount", T.StringType(), True),
                T.StructField("AttachmentsLastModified", T.StringType(), True),
                T.StructField("LastPublishedDate", T.StringType(), True),
                T.StructField("WelshLanguage", T.StringType(), True),
                T.StructField("CaseWorkType", T.StringType(), True),
                T.StructField("AttachmentModifyDate", T.StringType(), True),
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
            (
                20,
                20,
                "ADV-20",
                200,
                "EN010002",
                "Title 20",
                None,
                "From 20",
                "Agent 20",
                "email",
                "2025-02-01",
                "Enquiry 20",
                None,
                "Advice By 20",
                "2025-02-02",
                "Advice 20",
                None,
                "Published",
                None,
                "HA2",
                "Yes",
                "A",
                "B",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "0",
                "Horizon",
                None,
                "2025-02-01 00:00:00",
                None,
                "",
                "Y",
            ),
            (
                20,
                20,
                "ADV-20",
                200,
                "EN010002",
                "Title 20",
                None,
                "From 20",
                "Agent 20",
                "email",
                "2025-02-01",
                "Enquiry 20",
                None,
                "Advice By 20",
                "2025-02-02",
                "Advice 20",
                None,
                "Published",
                None,
                "HA1",
                "Yes",
                "A",
                "B",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "0",
                "Horizon",
                None,
                "2025-02-01 00:00:00",
                None,
                "",
                "Y",
            ),
        ],
        T.StructType(
            [
                T.StructField("NSIPAdviceID", T.IntegerType(), True),
                T.StructField("adviceId", T.IntegerType(), True),
                T.StructField("adviceReference", T.StringType(), True),
                T.StructField("caseId", T.IntegerType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("title", T.StringType(), True),
                T.StructField("titleWelsh", T.StringType(), True),
                T.StructField("from", T.StringType(), True),
                T.StructField("agent", T.StringType(), True),
                T.StructField("method", T.StringType(), True),
                T.StructField("enquiryDate", T.StringType(), True),
                T.StructField("enquiryDetails", T.StringType(), True),
                T.StructField("enquiryDetailsWelsh", T.StringType(), True),
                T.StructField("adviceGivenBy", T.StringType(), True),
                T.StructField("adviceDate", T.StringType(), True),
                T.StructField("adviceDetails", T.StringType(), True),
                T.StructField("adviceDetailsWelsh", T.StringType(), True),
                T.StructField("status", T.StringType(), True),
                T.StructField("redactionStatus", T.StringType(), True),
                T.StructField("attachmentIds", T.StringType(), True),
                T.StructField("Section51Advice", T.StringType(), True),
                T.StructField("EnquirerFirstName", T.StringType(), True),
                T.StructField("EnquirerLastName", T.StringType(), True),
                T.StructField("AdviceLastModified", T.StringType(), True),
                T.StructField("AttachmentCount", T.StringType(), True),
                T.StructField("AttachmentsLastModified", T.StringType(), True),
                T.StructField("LastPublishedDate", T.StringType(), True),
                T.StructField("WelshLanguage", T.StringType(), True),
                T.StructField("CaseWorkType", T.StringType(), True),
                T.StructField("AttachmentModifyDate", T.StringType(), True),
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

    horizon_deleted = spark.createDataFrame(
        [(20,)],
        ["advicenodeid"],
    )

    sb_advice_ids = spark.createDataFrame(
        [(10,)],
        ["adviceId"],
    )

    with (
        mock.patch(
            "odw.core.etl.transformation.harmonised.nsip_s51_advice_harmonisation_process.Util.get_storage_account",
            return_value="test_storage",
        ),
        mock.patch("odw.core.etl.transformation.harmonised.nsip_s51_advice_harmonisation_process.LoggingUtil"),
    ):
        inst = NsipS51AdviceHarmonisationProcess(spark)
        data_to_write, result = inst.process(
            source_data={
                "service_bus_data": service_bus_data,
                "horizon_data": horizon_data,
                "horizon_deleted": horizon_deleted,
                "sb_advice_ids": sb_advice_ids,
            }
        )

    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
    rows = [row.asDict(recursive=True) for row in actual_df.collect()]

    assert "SourceSystemID" not in actual_df.columns
    assert "AttachmentModifyDate" not in actual_df.columns
    assert actual_df.count() == 3

    advice_10_rows = [row for row in rows if row["adviceId"] == 10]
    advice_20_rows = [row for row in rows if row["adviceId"] == 20]

    assert len(advice_10_rows) == 1
    assert len(advice_20_rows) == 2

    assert advice_10_rows[0]["Migrated"] == "1"
    assert advice_10_rows[0]["IsActive"] == "Y"

    for row in advice_20_rows:
        assert row["Migrated"] == "0"
        assert row["IsActive"] == "N"
        assert row["ValidTo"] is not None
        assert row["attachmentIds"] == ["HA1", "HA2"]

    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert data_to_write[inst.OUTPUT_TABLE]["partition_by"] == ["IsActive"]
    assert result.metadata.insert_count == 3
