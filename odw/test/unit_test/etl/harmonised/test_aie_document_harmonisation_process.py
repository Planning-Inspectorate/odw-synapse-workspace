from odw.core.etl.transformation.harmonised.aie_document_harmonisation_process import AieDocumentHarmonisationProcess
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


def _make_horizon_row(
    temp_pk,
    document_id,
    filename,
    version,
    ingestion_date,
    case_ref="REF001",
):
    """Return a tuple matching the schema produced by _load_horizon_data()."""
    return (
        temp_pk,
        None,           # AIEDocumentDataID – always NULL from load
        document_id,
        case_ref,
        "DOC-REF",
        version,
        "EXAM-1",
        filename,
        filename,       # originalFilename
        "1000",
        "application/pdf",
        "http://example.com/doc",
        "/path/doc",
        "clean",
        "abc123",
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
        "0",            # Migrated
        "Horizon",      # ODTSourceSystem
        ingestion_date, # IngestionDate (timestamp string)
        None,           # ValidTo
        "",             # RowID
        "Y",            # IsActive
    )


_HORIZON_SCHEMA = T.StructType([
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
])


class TestAieDocumentHarmonisationProcess(SparkTestCase):
    """Unit tests for AieDocumentHarmonisationProcess.process().

    These tests exercise the transformation logic in isolation using an in-memory
    Spark session. No Synapse infrastructure is required.
    """

    def _run_process(self, horizon_data):
        """Helper: run process() with standard mocks and return (data_to_write, result)."""
        spark = PytestSparkSessionUtil().get_spark_session()
        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.transformation.harmonised.aie_document_harmonisation_process.LoggingUtil"),
        ):
            inst = AieDocumentHarmonisationProcess(spark)
            return inst.process(source_data={"horizon_data": horizon_data})

    # ------------------------------------------------------------------
    # Schema tests
    # ------------------------------------------------------------------

    def test__process__drops_temp_pk_from_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        output_columns = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].columns
        assert "TEMP_PK" not in output_columns

    def test__process__output_contains_required_columns(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        output_columns = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].columns
        for col in ("AIEDocumentDataID", "RowID", "IsActive", "ValidTo", "Migrated", "ODTSourceSystem", "IngestionDate"):
            assert col in output_columns, f"Expected column '{col}' in output"

    # ------------------------------------------------------------------
    # IsActive tests
    # ------------------------------------------------------------------

    def test__process__single_record_is_active(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        rows = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"

    def test__process__only_latest_record_is_active_for_same_pk(self):
        """Two ingestion dates for the same composite key: only the newer one is active."""
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00"),
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-02-01 00:00:00"),
            ],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        df = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = {row["IngestionDate"]: row for row in df.collect()}

        assert rows["2024-02-01 00:00:00"]["IsActive"] == "Y"
        assert rows["2024-01-01 00:00:00"]["IsActive"] == "N"

    def test__process__distinct_pks_are_both_active(self):
        """Two records with different composite keys are both independently active."""
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00"),
                _make_horizon_row("pk2", "2", "file2.pdf", "1", "2024-01-01 00:00:00"),
            ],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        df = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"]
        active_count = df.filter(df["IsActive"] == "Y").count()
        assert active_count == 2

    # ------------------------------------------------------------------
    # AIEDocumentDataID tests
    # ------------------------------------------------------------------

    def test__process__aie_document_data_id_assigned(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        rows = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        assert rows[0]["AIEDocumentDataID"] is not None
        assert rows[0]["AIEDocumentDataID"] == 1

    def test__process__aie_document_data_ids_are_sequential_and_unique(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00"),
                _make_horizon_row("pk2", "2", "file2.pdf", "1", "2024-01-02 00:00:00"),
            ],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        df = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"]
        ids = sorted([row["AIEDocumentDataID"] for row in df.collect()])
        assert ids == [1, 2]

    # ------------------------------------------------------------------
    # ValidTo tests
    # ------------------------------------------------------------------

    def test__process__valid_to_null_for_active_record(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        rows = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        assert rows[0]["ValidTo"] is None

    def test__process__valid_to_set_for_historical_record(self):
        """The historical record's ValidTo must equal the newer record's IngestionDate."""
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00"),
                _make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-02-01 00:00:00"),
            ],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        df = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"]
        rows = {row["IngestionDate"]: row for row in df.collect()}

        assert rows["2024-02-01 00:00:00"]["ValidTo"] is None
        assert rows["2024-01-01 00:00:00"]["ValidTo"] == "2024-02-01 00:00:00"

    # ------------------------------------------------------------------
    # RowID test
    # ------------------------------------------------------------------

    def test__process__row_id_is_populated(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, _ = self._run_process(horizon_data)
        rows = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]["data"].collect()
        # RowID must be a non-empty MD5 hex string (32 chars)
        assert rows[0]["RowID"] is not None
        assert len(rows[0]["RowID"]) == 32

    # ------------------------------------------------------------------
    # Write config tests
    # ------------------------------------------------------------------

    def test__process__write_config_is_correct(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        horizon_data = spark.createDataFrame(
            [_make_horizon_row("pk1", "1", "file1.pdf", "1", "2024-01-01 00:00:00")],
            _HORIZON_SCHEMA,
        )
        data_to_write, result = self._run_process(horizon_data)
        write_config = data_to_write[AieDocumentHarmonisationProcess.OUTPUT_TABLE]

        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1

    # ------------------------------------------------------------------
    # ETLProcessFactory registration test
    # ------------------------------------------------------------------

    def test__aie_document_harmonisation_process__is_registered_in_factory(self):
        from odw.core.etl.etl_process_factory import ETLProcessFactory
        process_class = ETLProcessFactory.get("aie-document-harmonised")
        assert process_class is AieDocumentHarmonisationProcess
