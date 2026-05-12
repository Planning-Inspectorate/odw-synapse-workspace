from datetime import datetime, timezone

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from odw.core.etl.etl_result import ETLSuccessResult


class LoggingUtil:
    @staticmethod
    def get_logger(name: str):
        import logging

        return logging.getLogger(name)


class NsipInvoiceHarmonisationProcess:
    SOURCE_TABLE = "odw_harmonised_db.sb_nsip_project"
    TARGET_TABLE = "odw_harmonised_db.sb_nsip_invoice"
    OUTPUT_TABLE = "sb_nsip_invoice"

    def __init__(self, spark):
        self.spark = spark
        self.logger = LoggingUtil.get_logger(__name__)
        self.input_data = {}
        self._run_ingestion_timestamp = None
        self._run_valid_to_timestamp = None

    @classmethod
    def get_name(cls):
        return "nsip_invoice_harmonisation_process"

    def get_input_table_names(self) -> list[str]:
        return ["sb_nsip_project", "sb_nsip_invoice"]

    def get_output_table_names(self) -> list[str]:
        return ["sb_nsip_invoice"]

    def _current_ingestion_timestamp(self):
        if self._run_ingestion_timestamp is None:
            self._run_ingestion_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+0000")
        return self._run_ingestion_timestamp

    def _current_valid_to_timestamp(self):
        if self._run_valid_to_timestamp is None:
            self._run_valid_to_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000000+0000")
        return self._run_valid_to_timestamp

    def _source_schema(self):
        invoice_schema = StructType(
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

        return StructType(
            [
                StructField("NSIPProjectInfoInternalID", LongType(), True),
                StructField("caseId", LongType(), True),
                StructField("caseReference", StringType(), True),
                StructField("invoices", ArrayType(invoice_schema), True),
                StructField("ODTSourceSystem", StringType(), True),
                StructField("SourceSystemID", StringType(), True),
                StructField("IngestionDate", StringType(), True),
            ]
        )

    def _target_schema(self):
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

    def _standardise_output(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("NSIPInvoiceID").cast("long").alias("NSIPInvoiceID"),
            F.col("caseId").cast("long").alias("caseId"),
            F.col("caseReference").cast("string").alias("caseReference"),
            F.col("invoiceStage").cast("string").alias("invoiceStage"),
            F.col("invoiceNumber").cast("string").alias("invoiceNumber"),
            F.col("amountDue").cast("double").alias("amountDue"),
            F.col("paymentDueDate").cast("string").alias("paymentDueDate"),
            F.col("invoicedDate").cast("string").alias("invoicedDate"),
            F.col("paymentDate").cast("string").alias("paymentDate"),
            F.col("refundCreditNoteNumber").cast("string").alias("refundCreditNoteNumber"),
            F.col("refundAmount").cast("double").alias("refundAmount"),
            F.col("refundIssueDate").cast("string").alias("refundIssueDate"),
            F.col("Migrated").cast("int").alias("Migrated"),
            F.col("ODTSourceSystem").cast("string").alias("ODTSourceSystem"),
            F.col("SourceSystemID").cast("string").alias("SourceSystemID"),
            F.col("IngestionDate").cast("string").alias("IngestionDate"),
            F.col("ValidTo").cast("string").alias("ValidTo"),
            F.col("RowID").cast("string").alias("RowID"),
            F.col("IsActive").cast("string").alias("IsActive"),
            F.col("NSIPProjectInfoInternalID").cast("long").alias("NSIPProjectInfoInternalID"),
        )

    def load_data(self, **kwargs):
        try:
            source_df = self.spark.table(self.SOURCE_TABLE)
        except Exception:
            source_df = self.spark.createDataFrame([], self._source_schema())

        try:
            target_df = self.spark.table(self.TARGET_TABLE)
        except Exception:
            target_df = self.spark.createDataFrame([], self._target_schema())

        self.input_data = {
            "sb_nsip_project": source_df,
            "sb_nsip_invoice": target_df,
        }
        return self.input_data

    def write_data(self, data_to_write):
        final_df = self._standardise_output(data_to_write[self.OUTPUT_TABLE]["data"])

        try:
            self.spark.sql(f"DELETE FROM {self.TARGET_TABLE}")
        except Exception:
            pass

        final_df.write.format("delta").mode("append").saveAsTable(self.TARGET_TABLE)

    def _is_empty_df(self, df: DataFrame) -> bool:
        return len(df.limit(1).collect()) == 0

    def _rowid_expr(self):
        ordered_cols = [
            "NSIPInvoiceID",
            "NSIPProjectInfoInternalID",
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
            "NewIsActive",
        ]
        exprs = [F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in ordered_cols]
        return F.md5(F.concat(*exprs))

    def _apply_rowid(self, df: DataFrame) -> DataFrame:
        return df.withColumn("NewIsActive", F.col("IsActive")).withColumn("RowID", self._rowid_expr()).drop("NewIsActive")

    def run(self):
        self._run_ingestion_timestamp = None
        self._run_valid_to_timestamp = None

        start_time = datetime.now(timezone.utc)

        self.load_data()
        data_to_write, result = self.process()
        self.write_data(data_to_write)

        end_time = datetime.now(timezone.utc)

        if result is None:
            insert_count = data_to_write[self.OUTPUT_TABLE]["data"].count()
            result = ETLSuccessResult(
                metadata={
                    "insert_count": insert_count,
                    "update_count": 0,
                    "start_execution_time": start_time.isoformat(),
                    "end_execution_time": end_time.isoformat(),
                    "activity_type": "nsip_invoice_harmonisation_process",
                    "duration_seconds": (end_time - start_time).total_seconds(),
                }
            )

        return result

    def process(self, source_data=None):
        start_time = datetime.now(timezone.utc)

        output_cols = [
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
        group_cols = ["caseId", "invoiceNumber"]

        if source_data is None:
            source_df = self.input_data["sb_nsip_project"].select(
                "NSIPProjectInfoInternalID",
                "caseId",
                "caseReference",
                "invoices",
                "ODTSourceSystem",
                "SourceSystemID",
                "IngestionDate",
            )
            existing_df = self._standardise_output(self.input_data["sb_nsip_invoice"])
        else:
            source_df = source_data["source_data"].select(
                "NSIPProjectInfoInternalID",
                "caseId",
                "caseReference",
                "invoices",
                "ODTSourceSystem",
                "SourceSystemID",
                "IngestionDate",
            )
            if source_data.get("target_exists") and source_data.get("target_data") is not None:
                existing_df = self._standardise_output(source_data["target_data"])
            else:
                existing_df = self.spark.createDataFrame([], self._target_schema())

        existing_max_ingestion = existing_df.select(F.max("IngestionDate").alias("max_ingestion")).collect()[0]["max_ingestion"]

        if existing_max_ingestion is not None:
            source_df = source_df.filter(F.col("IngestionDate") > F.lit(existing_max_ingestion))

        exploded_df = (
            source_df.filter(F.col("invoices").isNotNull())
            .withColumn("invoice", F.explode("invoices"))
            .select(
                F.col("NSIPProjectInfoInternalID").cast("long").alias("NSIPProjectInfoInternalID"),
                F.col("caseId").cast("long").alias("caseId"),
                F.col("caseReference").cast("string").alias("caseReference"),
                F.col("invoice.invoiceStage").cast("string").alias("invoiceStage"),
                F.col("invoice.invoiceNumber").cast("string").alias("invoiceNumber"),
                F.col("invoice.amountDue").cast("double").alias("amountDue"),
                F.col("invoice.paymentDueDate").cast("string").alias("paymentDueDate"),
                F.col("invoice.invoicedDate").cast("string").alias("invoicedDate"),
                F.col("invoice.paymentDate").cast("string").alias("paymentDate"),
                F.col("invoice.refundCreditNoteNumber").cast("string").alias("refundCreditNoteNumber"),
                F.col("invoice.refundAmount").cast("double").alias("refundAmount"),
                F.col("invoice.refundIssueDate").cast("string").alias("refundIssueDate"),
                F.when(
                    F.col("caseId").isNull() | F.col("invoice.invoiceNumber").isNull(),
                    F.lit(0),
                )
                .otherwise(F.lit(1))
                .cast("int")
                .alias("Migrated"),
                F.col("ODTSourceSystem").cast("string").alias("ODTSourceSystem"),
                F.col("SourceSystemID").cast("string").alias("SourceSystemID"),
                F.lit(self._current_ingestion_timestamp()).cast("string").alias("IngestionDate"),
                F.lit(None).cast("string").alias("ValidTo"),
                F.lit("Y").cast("string").alias("IsActive"),
            )
        )

        if self._is_empty_df(exploded_df):
            final_df = self._apply_rowid(existing_df.select(*output_cols)).persist(StorageLevel.MEMORY_AND_DISK)
            final_df.count()

            data_to_write = {
                self.OUTPUT_TABLE: {
                    "data": final_df,
                    "write_mode": "overwrite",
                }
            }

            end_time = datetime.now(timezone.utc)
            result = ETLSuccessResult(
                metadata={
                    "insert_count": 0,
                    "update_count": 0,
                    "start_execution_time": start_time.isoformat(),
                    "end_execution_time": end_time.isoformat(),
                    "activity_type": "nsip_invoice_harmonisation_process",
                    "duration_seconds": (end_time - start_time).total_seconds(),
                }
            )
            return data_to_write, result

        existing_max_id = existing_df.select(F.max("NSIPInvoiceID").alias("max_id")).collect()[0]["max_id"]
        next_id_start = 1 if existing_max_id is None else int(existing_max_id) + 1

        new_id_window = Window.orderBy(
            F.col("IngestionDate").asc_nulls_last(),
            F.col("NSIPProjectInfoInternalID").asc_nulls_last(),
            F.col("caseId").asc_nulls_last(),
            F.col("invoiceNumber").asc_nulls_last(),
            F.col("caseReference").asc_nulls_last(),
        )

        new_rows_df = exploded_df.withColumn(
            "NSIPInvoiceID",
            (F.row_number().over(new_id_window) + F.lit(next_id_start - 1)).cast("long"),
        ).select(
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
            "IsActive",
            "NSIPProjectInfoInternalID",
        )

        changed_groups_df = new_rows_df.select(*group_cols).distinct()

        existing_unchanged_df = existing_df.join(changed_groups_df, on=group_cols, how="left_anti").select(*output_cols)

        existing_changed_df = existing_df.join(changed_groups_df, on=group_cols, how="inner").select(*output_cols)

        candidates_df = existing_changed_df.unionByName(new_rows_df.withColumn("RowID", F.lit(None).cast("string")).select(*output_cols))

        precedence_window = Window.partitionBy(*group_cols).orderBy(
            F.col("NSIPProjectInfoInternalID").desc_nulls_last(),
            F.col("IngestionDate").desc_nulls_last(),
            F.col("NSIPInvoiceID").desc_nulls_last(),
        )

        resolved_changed_df = (
            candidates_df.withColumn("_rn", F.row_number().over(precedence_window))
            .withColumn(
                "IsActive",
                F.when(F.col("_rn") == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
            .withColumn(
                "ValidTo",
                F.when(F.col("_rn") == 1, F.lit(None).cast("string")).otherwise(F.lit(self._current_valid_to_timestamp()).cast("string")),
            )
            .drop("_rn")
            .select(*output_cols)
        )

        final_df = existing_unchanged_df.unionByName(resolved_changed_df).select(*output_cols)
        final_df = self._apply_rowid(final_df).persist(StorageLevel.MEMORY_AND_DISK)
        final_df.count()

        insert_count = new_rows_df.count()
        update_count = resolved_changed_df.where(F.col("IsActive") == "N").count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "write_mode": "overwrite",
            }
        }

        end_time = datetime.now(timezone.utc)
        result = ETLSuccessResult(
            metadata={
                "insert_count": insert_count,
                "update_count": update_count,
                "start_execution_time": start_time.isoformat(),
                "end_execution_time": end_time.isoformat(),
                "activity_type": "nsip_invoice_harmonisation_process",
                "duration_seconds": (end_time - start_time).total_seconds(),
            }
        )

        return data_to_write, result
