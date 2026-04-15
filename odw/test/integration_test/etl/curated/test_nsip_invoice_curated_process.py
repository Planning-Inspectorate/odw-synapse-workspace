import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    FloatType,
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


def _curated_schema():
    return StructType(
        [
            StructField("caseId", LongType(), True),
            StructField("caseReference", StringType(), True),
            StructField("invoiceStage", StringType(), True),
            StructField("invoiceNumber", StringType(), True),
            StructField("amountDue", FloatType(), True),
            StructField("paymentDueDate", StringType(), True),
            StructField("invoicedDate", StringType(), True),
            StructField("paymentDate", StringType(), True),
            StructField("refundCreditNoteNumber", StringType(), True),
            StructField("refundAmount", FloatType(), True),
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


def _curated_row(**overrides):
    row = {
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
    }
    row.update(overrides)
    return row


class TestNsipInvoiceCuratedProcess(ETLTestCase):
    def compare_curated_data(self, expected_df: DataFrame, actual_df: DataFrame):
        assert_dataframes_equal(expected_df, actual_df)

    def write_harmonised_table(self, spark, table_df: DataFrame):
        self.write_existing_table(
            spark,
            table_df,
            "sb_nsip_invoice",
            "odw_harmonised_db",
            "odw-harmonised",
            "ServiceBus/nsip_invoice",
            "overwrite",
        )

    def write_empty_curated_table(self, spark):
        empty_curated_df = spark.createDataFrame([], _curated_schema())
        self.write_existing_table(
            spark,
            empty_curated_df,
            "nsip_invoice",
            "odw_curated_db",
            "odw-curated",
            "ServiceBus/nsip_invoice",
            "overwrite",
        )

    def test__nsip_invoice_curated_process__run__initial_load_matches_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_invoice = spark.createDataFrame(
            [
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
            ],
            _harmonised_schema(),
        )
        self.write_harmonised_table(spark, harmonised_invoice)
        self.write_empty_curated_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(caseId=2001, invoiceNumber="INV-001"),
                _curated_row(
                    caseId=2002,
                    caseReference="EN010002",
                    invoiceNumber="INV-002",
                    amountDue="200.00",
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_invoice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.nsip_invoice")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_curated_process__run__drops_duplicate_active_rows_using_distinct_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_invoice = spark.createDataFrame(
            [
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
            ],
            _harmonised_schema(),
        )
        self.write_harmonised_table(spark, harmonised_invoice)
        self.write_empty_curated_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(caseId=2001, invoiceNumber="INV-001"),
            ],
            _curated_schema(),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_invoice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.nsip_invoice")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_curated_process__run__keeps_distinct_active_business_rows(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_invoice = spark.createDataFrame(
            [
                _harmonised_row(caseId=2001, invoiceNumber="INV-001", amountDue="100.50", IsActive="Y"),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=200,
                    caseId=2001,
                    invoiceNumber="INV-002",
                    amountDue="200.50",
                    IsActive="Y",
                ),
            ],
            _harmonised_schema(),
        )
        self.write_harmonised_table(spark, harmonised_invoice)
        self.write_empty_curated_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(caseId=2001, invoiceNumber="INV-001", amountDue="100.50"),
                _curated_row(caseId=2001, invoiceNumber="INV-002", amountDue="200.50"),
            ],
            _curated_schema(),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_invoice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.nsip_invoice")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_curated_process__run__preserves_null_business_values(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_invoice = spark.createDataFrame(
            [
                _harmonised_row(
                    paymentDate=None,
                    refundCreditNoteNumber=None,
                    refundAmount=None,
                    refundIssueDate=None,
                    IsActive="Y",
                ),
            ],
            _harmonised_schema(),
        )
        self.write_harmonised_table(spark, harmonised_invoice)
        self.write_empty_curated_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _curated_row(
                    paymentDate=None,
                    refundCreditNoteNumber=None,
                    refundAmount=None,
                    refundIssueDate=None,
                ),
            ],
            _curated_schema(),
        )

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_invoice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.nsip_invoice")
        self.compare_curated_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_curated_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_invoice = spark.createDataFrame([], _harmonised_schema())
        self.write_harmonised_table(spark, harmonised_invoice)
        self.write_empty_curated_table(spark)

        expected_table_data = spark.createDataFrame([], _curated_schema())

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_invoice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.curated.nsip_invoice_curated_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceCuratedProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_curated_db.nsip_invoice")
        self.compare_curated_data(expected_table_data, actual_table_data)
