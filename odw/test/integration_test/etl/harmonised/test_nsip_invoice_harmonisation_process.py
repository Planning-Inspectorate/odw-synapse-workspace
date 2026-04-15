import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process import NsipInvoiceHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Harmonisation logic not implemented yet")


def _invoice_struct():
    return StructType(
        [
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


def _invoice(**overrides):
    invoice = {
        "invoiceStage": "Submitted",
        "invoiceNumber": "INV-001",
        "amountDue": "100.50",
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
        "amountDue": "100.50",
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


class TestNsipInvoiceHarmonisationProcess(ETLTestCase):
    def compare_harmonised_data(self, expected_df: DataFrame, actual_df: DataFrame):
        assert_dataframes_equal(expected_df, actual_df)

    def write_source_table(self, spark, table_df: DataFrame):
        self.write_existing_table(
            spark,
            table_df,
            "sb_nsip_project",
            "odw_harmonised_db",
            "odw-harmonised",
            "ServiceBus/nsip_project",
            "overwrite",
        )

    def write_target_table(self, spark, table_df: DataFrame):
        self.write_existing_table(
            spark,
            table_df,
            "sb_nsip_invoice",
            "odw_harmonised_db",
            "odw-harmonised",
            "ServiceBus/nsip_invoice",
            "overwrite",
        )

    def write_empty_target_table(self, spark):
        empty_target_df = spark.createDataFrame([], _harmonised_schema())
        self.write_target_table(spark, empty_target_df)

    def test__nsip_invoice_harmonisation_process__run__initial_load_matches_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                invoices=[
                    _invoice(invoiceNumber="INV-001", amountDue="100.50"),
                    _invoice(invoiceNumber="INV-002", amountDue="200.00"),
                ]
            ),
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                invoices=None,
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    caseReference="EN010001",
                    invoiceStage="Submitted",
                    invoiceNumber="INV-001",
                    amountDue="100.50",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    caseReference="EN010001",
                    invoiceStage="Submitted",
                    invoiceNumber="INV-002",
                    amountDue="200.00",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__empty_invoice_array_produces_zero_rows_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(invoices=[]),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame([], _harmonised_schema())

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__preserves_duplicate_invoice_rows_like_legacy(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                invoices=[
                    _invoice(invoiceNumber="INV-DUP", amountDue="100.50"),
                    _invoice(invoiceNumber="INV-DUP", amountDue="100.50"),
                ]
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    caseReference="EN010001",
                    invoiceNumber="INV-DUP",
                    amountDue="100.50",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    RowID=None,
                    ValidTo="2025-02-01T10:00:00.000000+0000",
                    IsActive="N",
                    Migrated=1,
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    caseReference="EN010001",
                    invoiceNumber="INV-DUP",
                    amountDue="100.50",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__starts_surrogate_key_from_existing_max_id_plus_one(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                caseId=2002,
                caseReference="EN010002",
                invoices=[_invoice(invoiceNumber="INV-002")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            ),
            _source_row(
                caseId=2003,
                caseReference="EN010003",
                invoices=[_invoice(invoiceNumber="INV-003")],
                IngestionDate="2025-01-19T10:00:00.000000+0000",
            ),
        ]
        existing_rows = [
            _harmonised_row(NSIPInvoiceID=10, invoiceNumber="INV-001"),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(NSIPInvoiceID=10, invoiceNumber="INV-001"),
                _harmonised_row(
                    NSIPInvoiceID=11,
                    NSIPProjectInfoInternalID=100,
                    caseId=2002,
                    caseReference="EN010002",
                    invoiceNumber="INV-002",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
                _harmonised_row(
                    NSIPInvoiceID=12,
                    NSIPProjectInfoInternalID=100,
                    caseId=2003,
                    caseReference="EN010003",
                    invoiceNumber="INV-003",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__deactivates_older_row_when_newer_project_record_arrives(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                caseReference="EN010001",
                invoices=[_invoice(invoiceNumber="INV-001", amountDue="120.00")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            )
        ]
        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                amountDue="100.50",
                IsActive="Y",
                ValidTo=None,
            )
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    amountDue="100.50",
                    ValidTo="2025-02-01T11:00:00.000000+0000",
                    IsActive="N",
                    Migrated=1,
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=200,
                    caseId=2001,
                    caseReference="EN010001",
                    invoiceNumber="INV-001",
                    amountDue="120.00",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T11:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__same_case_different_invoice_number_keeps_both_active(
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
            )
        ]
        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                invoices=[_invoice(invoiceNumber="INV-999", amountDue="999.00")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            )
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    IsActive="Y",
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=200,
                    caseId=2001,
                    invoiceNumber="INV-999",
                    amountDue="999.00",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    IsActive="Y",
                    ValidTo=None,
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T11:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__active_row_is_chosen_by_project_info_id_not_ingestion_date(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=500,
                caseId=2001,
                invoiceNumber="INV-001",
                IngestionDate="2025-01-16T12:00:00.000000+0000",
                IsActive="Y",
            )
        ]
        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=300,
                caseId=2001,
                invoices=[_invoice(invoiceNumber="INV-001", amountDue="120.00")],
                IngestionDate="2025-01-20T10:00:00.000000+0000",
            )
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=500,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    IngestionDate="2025-01-16T12:00:00.000000+0000",
                    IsActive="Y",
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=300,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    amountDue="120.00",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo="2025-02-01T11:00:00.000000+0000",
                    IsActive="N",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T11:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__filters_out_rows_not_newer_than_existing_max_ingestion_date(
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
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=10,
                    IngestionDate="2025-01-16T12:00:00.000000+0000",
                ),
                _harmonised_row(
                    NSIPInvoiceID=11,
                    NSIPProjectInfoInternalID=100,
                    caseId=2002,
                    caseReference="EN010002",
                    invoiceNumber="INV-002",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__unrelated_existing_groups_remain_untouched(
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
            ),
            _harmonised_row(
                NSIPInvoiceID=2,
                NSIPProjectInfoInternalID=999,
                caseId=9001,
                caseReference="EN099001",
                invoiceNumber="INV-UNCHANGED",
                IsActive="Y",
                IngestionDate="2025-01-17T12:00:00.000000+0000",
            ),
        ]
        source_rows = [
            _source_row(
                NSIPProjectInfoInternalID=200,
                caseId=2001,
                invoices=[_invoice(invoiceNumber="INV-001", amountDue="120.00")],
                IngestionDate="2025-01-18T10:00:00.000000+0000",
            ),
        ]
        self.write_source_table(spark, spark.createDataFrame(source_rows, _standardised_schema()))
        self.write_target_table(spark, spark.createDataFrame(existing_rows, _harmonised_schema()))

        expected_table_data = spark.createDataFrame(
            [
                _harmonised_row(
                    NSIPInvoiceID=1,
                    NSIPProjectInfoInternalID=100,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    ValidTo="2025-02-01T11:00:00.000000+0000",
                    IsActive="N",
                    Migrated=1,
                ),
                _harmonised_row(
                    NSIPInvoiceID=2,
                    NSIPProjectInfoInternalID=999,
                    caseId=9001,
                    caseReference="EN099001",
                    invoiceNumber="INV-UNCHANGED",
                    IsActive="Y",
                    IngestionDate="2025-01-17T12:00:00.000000+0000",
                ),
                _harmonised_row(
                    NSIPInvoiceID=3,
                    NSIPProjectInfoInternalID=200,
                    caseId=2001,
                    invoiceNumber="INV-001",
                    amountDue="120.00",
                    IngestionDate="2025-02-01T10:00:00.000000+0000",
                    ValidTo=None,
                    IsActive="Y",
                    Migrated=1,
                ),
            ],
            _harmonised_schema(),
        )

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T11:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)

    def test__nsip_invoice_harmonisation_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        self.write_source_table(spark, spark.createDataFrame([], _standardised_schema()))
        self.write_empty_target_table(spark)

        expected_table_data = spark.createDataFrame([], _harmonised_schema())

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_ingestion_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
            mock.patch.object(
                NsipInvoiceHarmonisationProcess,
                "_current_valid_to_timestamp",
                return_value="2025-02-01T10:00:00.000000+0000",
            ),
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)
            result = inst.run()

        assert_etl_result_successful(result)
        actual_table_data = spark.table("odw_harmonised_db.sb_nsip_invoice")
        self.compare_harmonised_data(expected_table_data, actual_table_data)
