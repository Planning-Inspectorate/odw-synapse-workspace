import hashlib
import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    DoubleType,
)
from odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process import NsipInvoiceHarmonisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _invoice_struct():
    return StructType(
        [
            StructField("invoiceStage", StringType(), True),
            StructField("invoiceNumber", StringType(), True),
            StructField("amountDue", DoubleType(), True),
            StructField("paymentDueDate", StringType(), True),
            StructField("invoicedDate", StringType(), True),
            StructField("paymentDate", StringType(), True),
            StructField("refundCreditNoteNumber", StringType(), True),
            StructField("refundAmount", DoubleType(), True),
            StructField("refundIssueDate", StringType(), True),
        ]
    )


def _standardised_schema():
    return StructType(
        [
            StructField("NSIPProjectInfoInternalID", LongType(), True),
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("invoices", ArrayType(_invoice_struct()), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
        ]
    )


def _harmonised_schema():
    return StructType(
        [
            StructField("NSIPInvoiceID", LongType(), True),
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("invoiceStage", StringType(), True),
            StructField("invoiceNumber", StringType(), True),
            StructField("amountDue", DoubleType(), True),
            StructField("paymentDueDate", StringType(), True),
            StructField("invoicedDate", StringType(), True),
            StructField("paymentDate", StringType(), True),
            StructField("refundCreditNoteNumber", StringType(), True),
            StructField("refundAmount", DoubleType(), True),
            StructField("refundIssueDate", StringType(), True),
            StructField("Migrated", IntegerType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("NSIPProjectInfoInternalID", LongType(), True),
        ]
    )


def _invoice(**overrides):
    invoice = {
        "invoiceStage": "Submitted",
        "invoiceNumber": "INV-001",
        "amountDue": 100.50,
        "paymentDueDate": "2025-01-20",
        "invoicedDate": "2025-01-10",
        "paymentDate": None,
        "refundCreditNoteNumber": None,
        "refundAmount": None,
        "refundIssueDate": None,
    }
    invoice.update(overrides)
    return invoice


def _source_row(**overrides):
    row = {
        "NSIPProjectInfoInternalID": 100,
        "caseId": 2001,
        "caseReference": "EN010001",
        "invoices": [_invoice()],
        "ODTSourceSystem": "ODT",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-01-15T10:00:00.000000+0000",
    }
    row.update(overrides)
    return row


def _harmonised_row(**overrides):
    row = {
        "NSIPInvoiceID": 1,
        "NSIPProjectInfoInternalID": 100,
        "caseId": 2001,
        "caseReference": "EN010001",
        "invoiceStage": "Submitted",
        "invoiceNumber": "INV-001",
        "amountDue": 100.50,
        "paymentDueDate": "2025-01-20",
        "invoicedDate": "2025-01-10",
        "paymentDate": None,
        "refundCreditNoteNumber": None,
        "refundAmount": None,
        "refundIssueDate": None,
        "ODTSourceSystem": "ODT",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-01-16T12:00:00.000000+0000",
        "ValidTo": None,
        "RowID": "old-row-id",
        "IsActive": "Y",
        "Migrated": 1,
    }
    row.update(overrides)
    return row


def _expected_rowid(row):
    values = [
        row.get("NSIPInvoiceID"),
        row.get("NSIPProjectInfoInternalID"),
        row.get("caseId"),
        row.get("caseReference"),
        row.get("invoiceStage"),
        row.get("invoiceNumber"),
        row.get("amountDue"),
        row.get("paymentDueDate"),
        row.get("invoicedDate"),
        row.get("paymentDate"),
        row.get("refundCreditNoteNumber"),
        row.get("refundAmount"),
        row.get("refundIssueDate"),
        row.get("Migrated"),
        row.get("ODTSourceSystem"),
        row.get("SourceSystemID"),
        row.get("IngestionDate"),
        row.get("ValidTo"),
        row.get("NewIsActive"),
    ]
    joined = "".join(str(value) if value is not None else "." for value in values)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()


class TestNsipInvoiceHarmonisationProcess(SparkTestCase):
    def test__nsip_invoice_harmonisation_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = NsipInvoiceHarmonisationProcess(spark)

        assert inst.get_name() == "nsip_invoice_harmonisation_process"

    def test__nsip_invoice_harmonisation_process__process__explodes_invoice_array_and_extracts_fields(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                invoices=[
                    _invoice(invoiceNumber="INV-001", amountDue=100.50),
                    _invoice(invoiceNumber="INV-002", amountDue=200.00),
                ]
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert set(df.select("invoiceNumber").rdd.flatMap(lambda x: x).collect()) == {"INV-001", "INV-002"}
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__nsip_invoice_harmonisation_process__process__filters_out_rows_with_null_invoices(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(invoices=None),
            _source_row(caseId=2002, caseReference="EN010002", invoices=[_invoice(invoiceNumber="INV-002")]),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert result.metadata.insert_count == 1

    def test__nsip_invoice_harmonisation_process__process__empty_invoice_array_produces_zero_rows_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(invoices=[]),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__nsip_invoice_harmonisation_process__process__outputs_expected_columns_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame([_source_row()], schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "NSIPInvoiceID",
            "caseId",
            "caseReference",
            "invoiceStage",
            "invoiceNumber",
            "amountDue",
            "paymentDueDate",
            "invoicedDate",
            "paymentDate",
            "refundCreditNoteNumber",
            "refundAmount",
            "refundIssueDate",
            "Migrated",
            "ODTSourceSystem",
            "SourceSystemID",
            "IngestionDate",
            "ValidTo",
            "RowID",
            "IsActive",
            "NSIPProjectInfoInternalID",
        ]

    def test__nsip_invoice_harmonisation_process__process__assigns_surrogate_keys_starting_from_one_on_initial_load(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(invoices=[_invoice(invoiceNumber="INV-001")]),
            _source_row(caseId=2002, caseReference="EN010002", invoices=[_invoice(invoiceNumber="INV-002")]),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        ids = [row["NSIPInvoiceID"] for row in df.select("NSIPInvoiceID").orderBy("NSIPInvoiceID").collect()]

        assert ids == [1, 2]

    def test__nsip_invoice_harmonisation_process__process__assigns_surrogate_keys_from_existing_max_id_plus_one(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(NSIPInvoiceID=10, invoiceNumber="INV-000"),
        ]

        source_rows = [
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                NSIPProjectInfoInternalID=200,
                invoices=[_invoice(invoiceNumber="INV-002")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            ),
            _source_row(
                caseId=2003,
                caseReference="EN010003",
                NSIPProjectInfoInternalID=300,
                invoices=[_invoice(invoiceNumber="INV-003")],
                IngestionDate="2025-01-19T10:00:00.000000+0000",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        new_ids = [
            row["NSIPInvoiceID"] for row in df.where(F.col("caseId").isin(2002, 2003)).select("NSIPInvoiceID").orderBy("NSIPInvoiceID").collect()
        ]

        assert new_ids == [11, 12]
        assert result.metadata.insert_count == 2

    def test__nsip_invoice_harmonisation_process__process__filters_out_old_records_using_max_existing_ingestion_date(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                caseId=2001,
                invoices=[_invoice(invoiceNumber="INV-001")],
                IngestionDate="2025-01-15T10:00:00.000000+0000",
            ),
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                invoices=[_invoice(invoiceNumber="INV-002")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            ),
        ]

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=10,
                IngestionDate="2025-01-16T12:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert df.where((F.col("caseId") == 2001) & (F.col("NSIPInvoiceID") != 10)).count() == 0
        assert result.metadata.insert_count == 1

    def test__nsip_invoice_harmonisation_process__process__preserves_duplicate_source_rows_on_initial_load_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(invoices=[_invoice(invoiceNumber="INV-001")]),
            _source_row(invoices=[_invoice(invoiceNumber="INV-001")]),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 2
        assert result.metadata.insert_count == 2

    def test__nsip_invoice_harmonisation_process__process__marks_older_invoice_inactive_for_same_case_and_invoice_number(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
                ValidTo=None,
                IngestionDate="2025-01-16T12:00:00.000000+0000",
            )
        ]

        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                caseReference="EN010001",
                invoices=[_invoice(invoiceNumber="INV-001", amountDue=120.00)],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        old_row = df.where((F.col("caseId") == 2001) & (F.col("NSIPProjectInfoInternalID") == 100)).collect()[0]
        new_row = df.where((F.col("caseId") == 2001) & (F.col("NSIPProjectInfoInternalID") == 200)).collect()[0]

        assert old_row["IsActive"] == "N"
        assert old_row["ValidTo"] == "2025-02-01T11:00:00.000000+0000"
        assert new_row["IsActive"] == "Y"
        assert new_row["ValidTo"] is None
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 1

    def test__nsip_invoice_harmonisation_process__process__same_case_different_invoice_number_both_remain_active(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
                ValidTo=None,
            )
        ]

        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                caseReference="EN010001",
                invoices=[_invoice(invoiceNumber="INV-002", amountDue=300.00)],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("caseId") == 2001) & (F.col("invoiceNumber") == "INV-001") & (F.col("IsActive") == "Y")).count() == 1
        assert df.where((F.col("caseId") == 2001) & (F.col("invoiceNumber") == "INV-002") & (F.col("IsActive") == "Y")).count() == 1
        assert df.where((F.col("caseId") == 2001) & F.col("ValidTo").isNotNull()).count() == 0
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_harmonisation_process__process__higher_project_info_id_wins_even_if_ingestiondate_is_older(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=500,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
                ValidTo=None,
                IngestionDate="2025-01-20T12:00:00.000000+0000",
            )
        ]

        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=400,
                caseId=2001,
                caseReference="EN010001",
                invoices=[_invoice(invoiceNumber="INV-001", amountDue=120.00)],
                IngestionDate="2025-01-21T10:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("NSIPProjectInfoInternalID") == 500) & (F.col("IsActive") == "Y")).count() == 1
        assert df.where((F.col("NSIPProjectInfoInternalID") == 400) & (F.col("IsActive") == "N")).count() == 1

    def test__nsip_invoice_harmonisation_process__process__sets_migrated_to_zero_when_caseid_or_invoice_number_is_null(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                caseId=None,
                invoices=[_invoice(invoiceNumber="INV-001")],
            ),
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                invoices=[_invoice(invoiceNumber=None)],
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("Migrated") == 0).count() == 2

    def test__nsip_invoice_harmonisation_process__process__unrelated_existing_groups_remain_untouched(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
                ValidTo=None,
            ),
            _harmonised_row(
                NSIPInvoiceID=2,
                NSIPProjectInfoInternalID=999,
                caseId=3001,
                caseReference="EN030001",
                invoiceNumber="INV-999",
                amountDue=999.99,
                IsActive="Y",
                ValidTo=None,
            ),
        ]

        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                invoices=[_invoice(invoiceNumber="INV-001", amountDue=120.00)],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        untouched_row = df.where((F.col("caseId") == 3001) & (F.col("invoiceNumber") == "INV-999")).collect()[0]

        assert untouched_row["IsActive"] == "Y"
        assert untouched_row["ValidTo"] is None
        assert untouched_row["amountDue"] == 999.99

    def test__nsip_invoice_harmonisation_process__process__calculates_rowid_like_legacy_hash(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [_source_row()]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        fixed_ingestion = "2025-02-01T10:00:00.000000+0000"

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value=fixed_ingestion),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        expected = _expected_rowid(
            {
                "NSIPInvoiceID": 1,
                "NSIPProjectInfoInternalID": 100,
                "caseId": 2001,
                "caseReference": "EN010001",
                "invoiceStage": "Submitted",
                "invoiceNumber": "INV-001",
                "amountDue": 100.50,
                "paymentDueDate": "2025-01-20",
                "invoicedDate": "2025-01-10",
                "paymentDate": None,
                "refundCreditNoteNumber": None,
                "refundAmount": None,
                "refundIssueDate": None,
                "Migrated": 1,
                "ODTSourceSystem": "ODT",
                "SourceSystemID": "SRC-1",
                "IngestionDate": fixed_ingestion,
                "ValidTo": None,
                "NewIsActive": "Y",
            }
        )

        assert row["RowID"] == expected

    def test__nsip_invoice_harmonisation_process__process__uses_dot_for_nulls_in_rowid_hash(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                caseReference=None,
                invoices=[
                    _invoice(
                        paymentDate=None,
                        refundCreditNoteNumber=None,
                        refundAmount=None,
                        refundIssueDate=None,
                    )
                ],
                ODTSourceSystem=None,
                SourceSystemID=None,
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        fixed_ingestion = "2025-02-01T10:00:00.000000+0000"

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value=fixed_ingestion),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        expected = _expected_rowid(
            {
                "NSIPInvoiceID": 1,
                "NSIPProjectInfoInternalID": 100,
                "caseId": 2001,
                "caseReference": None,
                "invoiceStage": "Submitted",
                "invoiceNumber": "INV-001",
                "amountDue": 100.50,
                "paymentDueDate": "2025-01-20",
                "invoicedDate": "2025-01-10",
                "paymentDate": None,
                "refundCreditNoteNumber": None,
                "refundAmount": None,
                "refundIssueDate": None,
                "Migrated": 1,
                "ODTSourceSystem": None,
                "SourceSystemID": None,
                "IngestionDate": fixed_ingestion,
                "ValidTo": None,
                "NewIsActive": "Y",
            }
        )

        assert row["RowID"] == expected

    def test__nsip_invoice_harmonisation_process__process__rowid_changes_when_hashed_business_field_changes(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(caseId=2001, invoices=[_invoice(invoiceNumber="INV-001", amountDue=100.50)]),
            _source_row(caseId=2002, caseReference="EN010002", invoices=[_invoice(invoiceNumber="INV-001", amountDue=200.50)]),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        fixed_ingestion = "2025-02-01T10:00:00.000000+0000"

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value=fixed_ingestion),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        rows = df.select("caseId", "RowID").orderBy("caseId").collect()

        assert rows[0]["RowID"]
        assert rows[1]["RowID"]
        assert rows[0]["RowID"] != rows[1]["RowID"]

    def test__nsip_invoice_harmonisation_process__process__active_rows_keep_validto_null(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                caseId=2001,
                caseReference="EN010001",
                invoices=[_invoice(invoiceNumber="INV-001")],
            ),
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                invoices=[_invoice(invoiceNumber="INV-002")],
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("IsActive") == "Y") & F.col("ValidTo").isNotNull()).count() == 0

    def test__nsip_invoice_harmonisation_process__process__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_standardised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil"):
            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
