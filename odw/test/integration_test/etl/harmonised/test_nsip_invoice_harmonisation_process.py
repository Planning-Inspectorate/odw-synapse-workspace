import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import functions as F
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


def _existing_harmonised_row(**overrides):
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0
        assert set(df.select("invoiceNumber").rdd.flatMap(lambda x: x).collect()) == {"INV-001", "INV-002"}
        assert df.where(F.col("Migrated") == 1).count() == 2

    def test__nsip_invoice_harmonisation_process__run__empty_invoice_array_produces_zero_rows_like_legacy(
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

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("invoiceNumber") == "INV-DUP").count() == 2
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 1

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
            _existing_harmonised_row(NSIPInvoiceID=10, invoiceNumber="INV-001"),
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        new_ids = [
            row["NSIPInvoiceID"]
            for row in df.where(F.col("invoiceNumber").isin("INV-002", "INV-003")).select("NSIPInvoiceID").orderBy("NSIPInvoiceID").collect()
        ]

        assert new_ids == [11, 12]

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
            _existing_harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                amountDue="100.50",
                IsActive="Y",
                ValidTo=None,
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        old_row = df.where((F.col("caseId") == 2001) & (F.col("NSIPProjectInfoInternalID") == 100)).collect()[0]
        new_row = df.where((F.col("caseId") == 2001) & (F.col("NSIPProjectInfoInternalID") == 200)).collect()[0]

        assert old_row["IsActive"] == "N"
        assert old_row["ValidTo"] == "2025-02-01T11:00:00.000000+0000"
        assert new_row["IsActive"] == "Y"
        assert new_row["ValidTo"] is None
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 1

    def test__nsip_invoice_harmonisation_process__run__same_case_different_invoice_number_keeps_both_active(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _existing_harmonised_row(
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where((F.col("caseId") == 2001) & (F.col("invoiceNumber") == "INV-001") & (F.col("IsActive") == "Y")).count() == 1
        assert df.where((F.col("caseId") == 2001) & (F.col("invoiceNumber") == "INV-999") & (F.col("IsActive") == "Y")).count() == 1
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__nsip_invoice_harmonisation_process__run__active_row_is_chosen_by_project_info_id_not_ingestion_date(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _existing_harmonised_row(
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert (
            df.where(
                (F.col("caseId") == 2001)
                & (F.col("invoiceNumber") == "INV-001")
                & (F.col("NSIPProjectInfoInternalID") == 500)
                & (F.col("IsActive") == "Y")
            ).count()
            == 1
        )
        assert (
            df.where(
                (F.col("caseId") == 2001)
                & (F.col("invoiceNumber") == "INV-001")
                & (F.col("NSIPProjectInfoInternalID") == 300)
                & (F.col("IsActive") == "N")
            ).count()
            == 1
        )

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
            _existing_harmonised_row(
                NSIPInvoiceID=10,
                IngestionDate="2025-01-16T12:00:00.000000+0000",
            )
        ]

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("invoiceNumber") == "INV-002").count() == 1
        assert result.metadata.insert_count == 1

    def test__nsip_invoice_harmonisation_process__run__unrelated_existing_groups_remain_untouched(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        existing_rows = [
            _existing_harmonised_row(
                NSIPInvoiceID=1,
                NSIPProjectInfoInternalID=100,
                caseId=2001,
                invoiceNumber="INV-001",
                IsActive="Y",
            ),
            _existing_harmonised_row(
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

        source_data = {
            "source_data": spark.createDataFrame(source_rows, schema=_standardised_schema()),
            "target_data": spark.createDataFrame(existing_rows, schema=_harmonised_schema()),
            "target_exists": True,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T11:00:00.000000+0000"),
            ):
                inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        untouched_row = df.where((F.col("caseId") == 9001) & (F.col("invoiceNumber") == "INV-UNCHANGED")).collect()[0]

        assert untouched_row["IsActive"] == "Y"
        assert untouched_row["ValidTo"] is None
        assert untouched_row["NSIPProjectInfoInternalID"] == 999

    def test__nsip_invoice_harmonisation_process__run__empty_source_returns_empty_output(
        self,
    ):
        spark = PytestSparkSessionUtil().get_spark_session()

        empty_df = spark.createDataFrame([], schema=_standardised_schema())

        source_data = {
            "source_data": empty_df,
            "target_exists": False,
        }

        with (
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.nsip_invoice_harmonisation_process.LoggingUtil") as mock_process_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_process_logging.return_value = mock.Mock()

            inst = NsipInvoiceHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
                mock.patch.object(inst, "_current_ingestion_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
                mock.patch.object(inst, "_current_valid_to_timestamp", return_value="2025-02-01T10:00:00.000000+0000"),
            ):
                result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert df.count() == 0
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0
