import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from odw.core.etl.transformation.curated.nsip_invoice_curated_process import NsipInvoiceCuratedProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

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


class TestNsipInvoiceCuratedProcess(ETLTestCase):
    def test__nsip_invoice_curated_process__run__initial_load_matches_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(caseId=2001, invoiceNumber="INV-001", IsActive="Y"),
            _harmonised_row(
                NSIPInvoiceID=2,
                NSIPProjectInfoInternalID=200,
                caseId=2002,
                caseReference="EN010002",
                invoiceNumber="INV-002",
                amountDue="200.00",
                IsActive="Y",
            ),
            _harmonised_row(
                NSIPInvoiceID=3,
                NSIPProjectInfoInternalID=300,
                caseId=2003,
                caseReference="EN010003",
                invoiceNumber="INV-003",
                amountDue="300.00",
                IsActive="N",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-003").count() == 0

    def test__nsip_invoice_curated_process__run__drops_duplicate_active_rows_using_distinct_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
            ),
            _harmonised_row(
                NSIPInvoiceID=99,
                NSIPProjectInfoInternalID=999,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
                RowID="row-99",
                IngestionDate="2025-03-01T10:00:00.000000+0000",
            ),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_harmonised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__run__keeps_distinct_active_business_rows(
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

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("invoiceNumber") == "INV-001").count() == 1
        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__run__preserves_null_business_values(
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

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["paymentDate"] is None
        assert row["refundCreditNoteNumber"] is None
        assert row["refundAmount"] is None
        assert row["refundIssueDate"] is None
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_curated_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_harmonised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
