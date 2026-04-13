from odw.core.etl.transformation.curated.nsip_representation_curated_process import NsipRepresentationCuratedProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


class TestNSIPRepresentationCurationProcess(SparkTestCase):
    def test__nsip_representation_curated_process__process__applies_status_and_party_mappings(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_representations = spark.createDataFrame(
            [
                (
                    1,
                    "WEB-1",
                    "EX-1",
                    "EN010001",
                    100,
                    "New",
                    None,
                    True,
                    "redacted-1",
                    "user-1",
                    "notes-1",
                    "An Organisation",
                    "represented-1",
                    "rep-1",
                    "register-1",
                    "Other Statutory Consultees",
                    "2025-01-01",
                    ["A1"],
                ),
                (
                    2,
                    "WEB-2",
                    "EX-2",
                    "EN010002",
                    200,
                    "Do Not Publish",
                    "original-2",
                    False,
                    None,
                    None,
                    None,
                    "Myself",
                    "represented-2",
                    "rep-2",
                    "register-2",
                    "Local Authority",
                    "2025-02-01",
                    ["A2"],
                ),
            ],
            T.StructType(
                [
                    T.StructField("representationId", T.IntegerType(), True),
                    T.StructField("webreference", T.StringType(), True),
                    T.StructField("examinationLibraryRef", T.StringType(), True),
                    T.StructField("caseRef", T.StringType(), True),
                    T.StructField("caseId", T.IntegerType(), True),
                    T.StructField("status", T.StringType(), True),
                    T.StructField("originalRepresentation", T.StringType(), True),
                    T.StructField("redacted", T.BooleanType(), True),
                    T.StructField("redactedRepresentation", T.StringType(), True),
                    T.StructField("redactedBy", T.StringType(), True),
                    T.StructField("redactedNotes", T.StringType(), True),
                    T.StructField("representationFrom", T.StringType(), True),
                    T.StructField("representedId", T.StringType(), True),
                    T.StructField("representativeId", T.StringType(), True),
                    T.StructField("registerFor", T.StringType(), True),
                    T.StructField("representationType", T.StringType(), True),
                    T.StructField("dateReceived", T.StringType(), True),
                    T.StructField("attachmentIds", T.ArrayType(T.StringType()), True),
                ]
            ),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_representation_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.transformation.curated.nsip_representation_curated_process.LoggingUtil"),
        ):
            inst = NsipRepresentationCuratedProcess(spark)
            data_to_write, result = inst.process(
                source_data={
                    "harmonised_representations": harmonised_representations,
                }
            )

        actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = {row["representationId"]: row.asDict(recursive=True) for row in actual_df.collect()}

        assert actual_df.count() == 2

        assert rows[1]["referenceId"] == "WEB-1"
        assert rows[1]["status"] == "awaiting_review"
        assert rows[1]["originalRepresentation"] == ""
        assert rows[1]["representationFrom"] == "ORGANISATION"
        assert rows[1]["registerFor"] == "ORGANISATION"
        assert rows[1]["representationType"] == "Statutory Consultees"

        assert rows[2]["status"] == "invalid"
        assert rows[2]["representationFrom"] == "PERSON"
        assert rows[2]["registerFor"] == "PERSON"
        assert rows[2]["representationType"] == "Local Authority"

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_representation"
        assert result.metadata.insert_count == 2
