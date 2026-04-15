import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from odw.core.etl.transformation.curated.nsip_invoice_curated_process import NsipInvoiceCuratedProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


def _harmonised_schema():
    return StructType(
        [
            StructField("NSIPInvoiceID", LongType(), True),
            StructField("NSIPProjectInfoInternalID", LongType(), True),
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("invoiceStage", StringType(), True),
            StructField("invoiceNumber", StringType(), True),
            StructField("amountDue", StringType(), True),
            StructField("paymentDueDate", StringType(), True),
            StructField("invoicedDate", StringType(), True),
            StructField("paymentDate", StringType(), True),
            StructField("refundCreditNoteNumber", StringType(), True),
            StructField("refundAmount", StringType(), True),
            StructField("refundIssueDate", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("SourceSystemID", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
            StructField("Migrated", IntegerType(), True),
        ]
    )


def _curated_schema():
    return StructType(
        [
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("invoiceStage", StringType(), True),
            StructField("invoiceNumber", StringType(), True),
            StructField("amountDue", StringType(), True),
            StructField("paymentDueDate", StringType(), True),
            StructField("invoicedDate", StringType(), True),
            StructField("paymentDate", StringType(), True),
            StructField("refundCreditNoteNumber", StringType(), True),
            StructField("refundAmount", StringType(), True),
            StructField("refundIssueDate", StringType(), True),
        ]
    )


def _harmonised_row(**overrides):
    row = {
        "NSIPInvoiceID": 1,
        "NSIPProjectInfoInternalID": 100,
        "caseId": 2001,
        "caseReference": "EN010001",
        "invoiceStage": "Submitted",
        "invoiceNumber": "INV-001",
        "amountDue": "100.50",
        "paymentDueDate": "2025-01-20",
        "invoicedDate": "2025-01-10",
        "paymentDate": None,
        "refundCreditNoteNumber": None,
        "refundAmount": None,
        "refundIssueDate": None,
        "ODTSourceSystem": "ODT",
        "SourceSystemID": "SRC-1",
        "IngestionDate": "2025-02-01T10:00:00.000000+0000",
        "ValidTo": None,
        "RowID": "row-1",
        "IsActive": "Y",
        "Migrated": 1,
    }
    row.update(overrides)
    return row


class TestNsipInvoiceCuratedProcess(SparkTestCase):
    def test__nsip_invoice_curated_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = NsipInvoiceCuratedProcess(spark)

        assert inst.get_name() == "nsip_invoice_curated_process"

    def test__nsip_invoice_curated_process__process__keeps_only_active_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(caseId=2001, invoiceNumber="INV-001", IsActive="Y"),
            _harmonised_row(caseId=2002, caseReference="EN010002", invoiceNumber="INV-002", IsActive="N"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 0
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__process__outputs_expected_curated_columns_only(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = {
            "source_data": spark.createDataFrame(
                [_harmonised_row()],
                schema=_harmonised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
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
        ]

    def test__nsip_invoice_curated_process__process__drops_duplicate_active_rows_using_distinct_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        duplicate_row = _harmonised_row(
            NSIPInvoiceID=1,
            NSIPProjectInfoInternalID=100,
            caseId=2001,
            invoiceNumber="INV-001",
            IsActive="Y",
        )

        duplicate_row_2 = _harmonised_row(
            NSIPInvoiceID=2,
            NSIPProjectInfoInternalID=101,
            caseId=2001,
            invoiceNumber="INV-001",
            IsActive="Y",
            RowID="row-2",
            IngestionDate="2025-02-02T10:00:00.000000+0000",
        )

        source_data = {
            "source_data": spark.createDataFrame(
                [duplicate_row, duplicate_row_2],
                schema=_harmonised_schema(),
            ),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__process__keeps_distinct_business_rows_when_values_differ(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(caseId=2001, invoiceNumber="INV-001", amountDue="100.50", IsActive="Y"),
            _harmonised_row(
                NSIPInvoiceID=2,
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                invoiceNumber="INV-002",
                amountDue="200.50",
                IsActive="Y",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__process__ignores_harmonised_metadata_columns_in_distinct_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                RowID="row-1",
                IngestionDate="2025-02-01T10:00:00.000000+0000",
                IsActive="Y",
            ),
            _harmonised_row(
                NSIPInvoiceID=99,
                NSIPProjectInfoInternalID=999,
                RowID="row-99",
                IngestionDate="2025-03-01T10:00:00.000000+0000",
                IsActive="Y",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__process__preserves_null_business_values(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                paymentDate=None,
                refundCreditNoteNumber=None,
                refundAmount=None,
                refundIssueDate=None,
                IsActive="Y",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["paymentDate"] is None
        assert row["refundCreditNoteNumber"] is None
        assert row["refundAmount"] is None
        assert row["refundIssueDate"] is None
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__process__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_harmonised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil"):
            inst = NsipInvoiceCuratedProcess(spark)
            data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
