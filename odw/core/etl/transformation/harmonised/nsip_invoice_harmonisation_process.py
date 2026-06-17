from typing import Dict, Any
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.io.synapse_delta_io import SynapseDeltaIO
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql.utils import AnalysisException
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType
import pyspark.sql.functions as F
from datetime import datetime, date


class NsipInvoiceHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "sb_nsip_invoice"
    SERVICE_BUS_TABLE = "sb_nsip_project"
    HARMONISED_DB = "odw_harmonised_db"
    _INCREMENTAL_KEY = "NSIPInvoiceID"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
    @classmethod
    def get_name(cls) -> str:
        return "NSIP Invoice Harmonisation Process"

    def _load_standardised_nsip_project(self):
        return self.spark.sql(f"""
            SELECT 
                NSIPProjectInfoInternalID,
                caseId,
                caseReference,
                invoices,
                ODTSourceSystem,
                SourceSystemID,
                IngestionDate
            FROM {self.HARMONISED_DB}.{self.SERVICE_BUS_TABLE}
            WHERE invoices IS NOT NULL
        """)

    def _load_harmonised_nsip_invoice(self):
        return SynapseDeltaIO().read(
            spark=self.spark,
            storage_endpoint=Util.get_storage_account(),
            container_name="odw-harmonised",
            blob_path=self.OUTPUT_TABLE,
        )

    def load_data(self) -> dict[str, Any]:
        try:
            existing_table = self._load_harmonised_nsip_invoice()
        except AnalysisException:
            existing_table = None
        return {"source_data": self._load_standardised_nsip_project(), "target_data": existing_table, "target_exists": bool(existing_table)}

    def _get_max_ingestion_date_to(self, existing_data: DataFrame):
        max_ingestion_date_to = existing_data.agg(F.max("IngestionDate")).collect()[0][0]
        if max_ingestion_date_to is None:
            max_ingestion_date_to = datetime(1900, 1, 1)
        return max_ingestion_date_to

    def _extract_invoice_fields(self, service_bus_data: DataFrame):
        # Explode invoices array
        exploded_df = service_bus_data.select(
            F.col("NSIPProjectInfoInternalID"),
            F.col("caseId"),
            F.col("caseReference"),
            F.explode(F.col("invoices")).alias("invoice"),
            F.col("ODTSourceSystem"),
            F.col("SourceSystemID"),
            F.col("IngestionDate"),
        )

        # Extract invoice fields
        return exploded_df.select(
            F.col("NSIPProjectInfoInternalID"),
            F.col("caseId"),
            F.col("caseReference"),
            F.col("invoice.invoiceStage").alias("invoiceStage"),
            F.col("invoice.invoiceNumber").alias("invoiceNumber"),
            F.col("invoice.amountDue").alias("amountDue"),
            F.col("invoice.paymentDueDate").alias("paymentDueDate"),
            F.col("invoice.invoicedDate").alias("invoicedDate"),
            F.col("invoice.paymentDate").alias("paymentDate"),
            F.col("invoice.refundCreditNoteNumber").alias("refundCreditNoteNumber"),
            F.col("invoice.refundAmount").alias("refundAmount"),
            F.col("invoice.refundIssueDate").alias("refundIssueDate"),
            F.col("ODTSourceSystem"),
            F.col("SourceSystemID"),
            F.col("IngestionDate"),
        )

    def _get_max_id_from_existing_table(self, existing_data: DataFrame):
        if not existing_data:
            return 0
        max_id = existing_data.agg(F.max(F.col(self._INCREMENTAL_KEY))).collect()[0][0]
        if not max_id:
            return 0
        return max_id

    def _harmonise(self, existing_data: DataFrame, service_bus_data: DataFrame):
        # Explode invoices array and Extract invoice fields
        new_data = self._extract_invoice_fields(service_bus_data)
        # Get max ID from existing table
        max_id_row = self._get_max_id_from_existing_table(existing_data)
        # Filter only records newer than max IngestionDate
        max_ingestion_date_to = self._get_max_ingestion_date_to(existing_data)
        new_data = new_data.filter(F.col("IngestionDate") > F.lit(max_ingestion_date_to))
        # Drop old IngestionDate column
        new_data = new_data.drop("IngestionDate")
        # Add new IngestionDate column with current timestamp (formatted)
        new_data = new_data.withColumn(
            "IngestionDate", F.concat(F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), F.lit("+0000"))
        )
        # Add extra columns and Add surrogate key starting from max_id_row + 1
        window_spec = Window.orderBy(F.monotonically_increasing_id())
        new_data = new_data.sort("caseReference")  # todo remove
        new_data = new_data.withColumns(
            {
                "ValidTo": F.lit(None).cast("string"),
                "RowID": F.lit(None).cast("string"),
                "IsActive": F.lit("Y").cast("string"),  # Default IsActive = "Y"
                self._INCREMENTAL_KEY: (F.row_number().over(window_spec) + F.lit(max_id_row)).cast("long"),
            }
        )
        # Reorder columns so incremental_key is first
        cols = new_data.columns
        reordered_cols = [self._INCREMENTAL_KEY] + [c for c in cols if c != self._INCREMENTAL_KEY]
        return new_data.select(reordered_cols)

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("source_data", source_data)
        existing_data: DataFrame = source_data.get("target_data", None)
        if not existing_data:
            existing_data = self.spark.createDataFrame(
                [],
                schema=StructType(
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
                ),
            )
        new_data = self._harmonise(existing_data, service_bus_data)
        # Delta update logic
        delta_update_col = "temp_harmonisation_update_col"
        # Mark old rows to be updated
        existing_data = existing_data.withColumn(delta_update_col, F.lit("update"))
        # Mark new rows to be inserted
        new_data = new_data.withColumns(
            {
                delta_update_col: F.lit("create"),
                "Migrated": F.lit(None),  # Default value just to align the dataframes, will be updated afterwards
            }
        )
        combined_data = existing_data.select(new_data.columns).union(new_data)
        window_spec = Window.partitionBy("caseId", "invoiceNumber").orderBy(F.col("NSIPProjectInfoInternalID").desc())
        combined_data = (
            combined_data.withColumn("row_num", F.row_number().over(window_spec))
            .withColumns(
                {
                    "Migrated": F.when(F.col("caseId").isNotNull() & F.col("invoiceNumber").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
                    "IsActive": F.when(F.col("row_num") == 1, F.lit("Y")).otherwise(F.lit("N")),
                }
            )
            # Must be done in a separate withColumn due to dependency on other columns
            .withColumn(
                "RowID",
                F.md5(
                    F.concat_ws(
                        "",
                        F.coalesce(F.col("NSIPInvoiceID").cast("string"), F.lit(".")),
                        F.coalesce(F.col("NSIPProjectInfoInternalID").cast("string"), F.lit(".")),
                        F.coalesce(F.col("caseId").cast("string"), F.lit(".")),
                        F.coalesce(F.col("caseReference").cast("string"), F.lit(".")),
                        F.coalesce(F.col("invoiceStage").cast("string"), F.lit(".")),
                        F.coalesce(F.col("invoiceNumber").cast("string"), F.lit(".")),
                        F.coalesce(F.col("amountDue").cast("string"), F.lit(".")),
                        F.coalesce(F.col("paymentDueDate").cast("string"), F.lit(".")),
                        F.coalesce(F.col("invoicedDate").cast("string"), F.lit(".")),
                        F.coalesce(F.col("paymentDate").cast("string"), F.lit(".")),
                        F.coalesce(F.col("refundCreditNoteNumber").cast("string"), F.lit(".")),
                        F.coalesce(F.col("refundAmount").cast("string"), F.lit(".")),
                        F.coalesce(F.col("refundIssueDate").cast("string"), F.lit(".")),
                        F.coalesce(F.col("Migrated").cast("string"), F.lit(".")),
                        F.coalesce(F.col("ODTSourceSystem").cast("string"), F.lit(".")),
                        F.coalesce(F.col("SourceSystemID").cast("string"), F.lit(".")),
                        F.coalesce(F.col("IngestionDate").cast("string"), F.lit(".")),
                        F.coalesce(F.col("ValidTo").cast("string"), F.lit(".")),
                        F.coalesce(F.col("IsActive").cast("string"), F.lit(".")),
                    )
                ),
            )
            .withColumn("ValidTo", F.when(F.col("IsActive") == "N", F.current_timestamp()).otherwise(F.col("ValidTo")))
            .drop("row_num")
        )
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": combined_data,
                "storage_kind": "ADLSG2-Delta",
                "database_name": "odw_harmonised_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": self.OUTPUT_TABLE,
                "merge_keys": ("caseId", "invoiceNumber", "IngestionDate", "NSIPProjectInfoInternalID"),
                "update_key_col": delta_update_col,
                "columns_to_update": ["IsActive", "ValidTo", "Migrated", "RowID"],
            }
        }
        insert_count = new_data.count()
        update_count = combined_data.filter(F.to_date("ValidTo") == F.lit(date.today())).count()
        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=update_count,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
