from datetime import datetime, timezone
from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class NsipInvoiceHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "sb_nsip_invoice"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "nsip_invoice_harmonisation_process"

    def _current_ingestion_timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def _empty_target_df(self) -> DataFrame:
        schema = StructType(
            [
                StructField("NSIPInvoiceID", LongType(), False),
                StructField("CaseId", StringType(), True),
                StructField("InvoiceNumber", StringType(), True),
                StructField("SupplierReference", StringType(), True),
                StructField("Description", StringType(), True),
                StructField("Amount", StringType(), True),
                StructField("CreatedDate", StringType(), True),
                StructField("ModifiedDate", StringType(), True),
                StructField("DeletedFlag", StringType(), True),
                StructField("IngestionDate", StringType(), True),
                StructField("Migrated", IntegerType(), True),
                StructField("ValidFrom", StringType(), True),
                StructField("ValidTo", StringType(), True),
                StructField("IsActive", StringType(), True),
                StructField("RowID", StringType(), True),
            ]
        )
        return self.spark.createDataFrame([], schema)

    def _table_exists(self, table_name: str) -> bool:
        try:
            return bool(self.spark.catalog.tableExists(table_name))
        except Exception:
            return False

    def _read_target_df(self) -> DataFrame:
        target_table = f"odw_harmonised_db.{self.OUTPUT_TABLE}"
        if self._table_exists(target_table):
            return self.spark.table(target_table)
        return self._empty_target_df()

    def load_data(self) -> dict[str, Any]:
        source_df = self.spark.table("odw_harmonised_db.sb_nsip_project")
        target_df = self._read_target_df()
        return {
            "source_df": source_df,
            "target_df": target_df,
        }

    def _build_row_id(self, df: DataFrame) -> DataFrame:
        row_id_columns = [
            "CaseId",
            "InvoiceNumber",
            "SupplierReference",
            "Description",
            "Amount",
            "CreatedDate",
            "ModifiedDate",
            "DeletedFlag",
            "IngestionDate",
            "Migrated",
            "ValidFrom",
            "ValidTo",
            "IsActive",
        ]

        exprs = [F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in row_id_columns]
        return df.withColumn("RowID", F.md5(F.concat_ws("||", *exprs)))

    def _transform_source(self, source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        current_ts = self._current_ingestion_timestamp()

        max_target_ingestion = target_df.select(F.max(F.col("IngestionDate")).alias("max_ingestion")).collect()[0]["max_ingestion"]

        exploded_df = (
            source_df
            .filter(F.col("invoices").isNotNull())
            .withColumn("invoice", F.explode(F.col("invoices")))
            .select(
                F.col("caseId").cast("string").alias("CaseId"),
                F.col("invoice.invoiceNumber").cast("string").alias("InvoiceNumber"),
                F.col("invoice.supplierReference").cast("string").alias("SupplierReference"),
                F.col("invoice.description").cast("string").alias("Description"),
                F.col("invoice.amount").cast("string").alias("Amount"),
                F.col("invoice.createdDate").cast("string").alias("CreatedDate"),
                F.col("invoice.modifiedDate").cast("string").alias("ModifiedDate"),
                F.col("invoice.deleted").cast("string").alias("DeletedFlag"),
                F.col("ingestionDate").cast("string").alias("IngestionDate"),
            )
        )

        if max_target_ingestion is not None:
            exploded_df = exploded_df.filter(F.col("IngestionDate") > F.lit(max_target_ingestion))

        existing_max_id = target_df.select(F.max(F.col("NSIPInvoiceID")).alias("max_id")).collect()[0]["max_id"]
        start_id = 0 if existing_max_id is None else int(existing_max_id)

        id_window = Window.orderBy(
            F.col("CaseId"),
            F.col("InvoiceNumber"),
            F.col("IngestionDate"),
        )

        new_rows_df = (
            exploded_df
            .withColumn("NSIPInvoiceID", (F.row_number().over(id_window) + F.lit(start_id)).cast("long"))
            .withColumn("Migrated", F.lit(1))
            .withColumn("ValidFrom", F.lit(current_ts))
            .withColumn("ValidTo", F.lit(None).cast("string"))
            .withColumn("IsActive", F.lit("Y"))
            .select(
                "NSIPInvoiceID",
                "CaseId",
                "InvoiceNumber",
                "SupplierReference",
                "Description",
                "Amount",
                "CreatedDate",
                "ModifiedDate",
                "DeletedFlag",
                "IngestionDate",
                "Migrated",
                "ValidFrom",
                "ValidTo",
                "IsActive",
            )
        )

        if new_rows_df.rdd.isEmpty():
            return target_df

        combined_df = target_df.select(
            "NSIPInvoiceID",
            "CaseId",
            "InvoiceNumber",
            "SupplierReference",
            "Description",
            "Amount",
            "CreatedDate",
            "ModifiedDate",
            "DeletedFlag",
            "IngestionDate",
            "Migrated",
            "ValidFrom",
            "ValidTo",
            "IsActive",
            "RowID",
        ).unionByName(
            new_rows_df.withColumn("RowID", F.lit(None).cast("string")),
            allowMissingColumns=True,
        )

        active_window = Window.partitionBy("CaseId", "InvoiceNumber").orderBy(
            F.col("IngestionDate").desc(),
            F.col("NSIPInvoiceID").desc(),
        )

        final_df = (
            combined_df
            .withColumn("rn", F.row_number().over(active_window))
            .withColumn("IsActive", F.when(F.col("rn") == 1, F.lit("Y")).otherwise(F.lit("N")))
            .withColumn(
                "ValidTo",
                F.when(F.col("rn") == 1, F.lit(None).cast("string")).otherwise(F.lit(current_ts))
            )
            .drop("rn")
        )

        final_df = self._build_row_id(final_df)

        return final_df.select(
            "NSIPInvoiceID",
            "CaseId",
            "InvoiceNumber",
            "SupplierReference",
            "Description",
            "Amount",
            "CreatedDate",
            "ModifiedDate",
            "DeletedFlag",
            "IngestionDate",
            "Migrated",
            "ValidFrom",
            "ValidTo",
            "IsActive",
            "RowID",
        )

    def process(self, source_data: dict[str, Any]):
        source_df = source_data["source_df"]
        target_df = source_data["target_df"]

        final_df = self._transform_source(source_df, target_df)

        return {
            "dataframe": final_df,
            "output_table": self.OUTPUT_TABLE,
            "database": "odw_harmonised_db",
            "write_mode": "overwrite",
        }