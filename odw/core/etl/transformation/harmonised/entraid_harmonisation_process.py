from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple

_ENTRAID_ROW_ID_COLUMNS = ["id", "employeeId", "givenName", "surname", "userPrincipalName"]


class EntraIdHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising EntraID user data from the standardised table.

    Implements the Load → Transform → Write pattern, replacing the ad-hoc cell logic
    previously contained in the entraid notebook.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "EntraID Harmonisation",
        "debug": False
    }
    ```
    """

    STD_TABLE = "odw_standardised_db.entraid"
    HRM_TABLE = "odw_harmonised_db.entraid"
    SOURCE_SYSTEM_TABLE = "odw_harmonised_db.main_sourcesystem_fact"
    OUTPUT_TABLE = "odw_harmonised_db.entraid"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "EntraID Harmonisation"

    # ------------------------------------------------------------------
    # load_data – all reads happen here, no joins or transformations
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load the latest EntraID records from the standardised table, the current
        harmonised table, and the source system fact table.
        """
        LoggingUtil().log_info(f"Loading EntraID standardised data from {self.STD_TABLE}")
        std_data = self.spark.sql(f"""
            SELECT DISTINCT
                id,
                employeeId,
                givenName,
                surname,
                userPrincipalName,
                to_timestamp(expected_from) AS IngestionDate
            FROM {self.STD_TABLE}
            WHERE expected_from = (SELECT MAX(expected_from) FROM {self.STD_TABLE})
            AND id IS NOT NULL
        """)

        LoggingUtil().log_info(f"Loading current harmonised data from {self.HRM_TABLE}")
        hrm_data = self.spark.sql(f"SELECT * FROM {self.HRM_TABLE}")

        LoggingUtil().log_info(f"Loading source system fact from {self.SOURCE_SYSTEM_TABLE}")
        source_system = self.spark.sql(f"""
            SELECT SourceSystemID
            FROM {self.SOURCE_SYSTEM_TABLE}
            WHERE Description = 'SAP HR' AND IsActive = 'Y'
        """)

        return {
            "std_data": std_data,
            "hrm_data": hrm_data,
            "source_system": source_system,
        }

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply all harmonisation transformations:

        1. Compute RowID for each incoming standardised record.
        2. Identify new and changed records by comparing against active harmonised records.
        3. Build new rows (IsActive='Y') for new/changed records.
        4. Close old versions of changed records (IsActive='N', ValidTo=today-1).
        5. Preserve unchanged active records and all historical inactive records.
        6. Reassign EmployeeEntraId via ROW_NUMBER() and write as INSERT OVERWRITE.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        std_data: DataFrame = self.load_parameter("std_data", source_data)
        hrm_data: DataFrame = self.load_parameter("hrm_data", source_data)
        source_system: DataFrame = self.load_parameter("source_system", source_data)

        source_system_rows = source_system.collect()
        source_system_id = source_system_rows[0]["SourceSystemID"] if source_system_rows else None

        # Step 1: Compute RowID for standardised records
        row_id_expr = F.md5(F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in _ENTRAID_ROW_ID_COLUMNS]))
        std_with_row_id = std_data.withColumn("RowID", row_id_expr)

        hrm_active = hrm_data.filter(F.col("IsActive") == "Y")

        # Step 2: Find new records (not in hrm) and changed records (hash mismatch)
        LoggingUtil().log_info("Identifying new and changed EntraID records")
        candidates = (
            std_with_row_id.alias("std")
            .join(hrm_active.select("id", "RowID", "IsActive").alias("hrm"), F.col("std.id") == F.col("hrm.id"), "left")
            .filter(F.col("hrm.id").isNull() | (F.col("std.RowID") != F.col("hrm.RowID")))
            .select(
                F.col("std.id"),
                F.col("std.employeeId"),
                F.col("std.givenName"),
                F.col("std.surname"),
                F.col("std.userPrincipalName"),
                F.col("std.IngestionDate"),
                F.col("std.RowID"),
                F.col("hrm.IsActive").alias("HistoricIsActive"),
            )
        )

        # Step 3: New rows to insert with IsActive='Y'
        LoggingUtil().log_info("Building new/updated rows")
        output_cols = ["EmployeeEntraId", "id", "employeeId", "givenName", "surname", "userPrincipalName",
                       "Migrated", "ODTSourceSystem", "SourceSystemID", "IngestionDate", "ValidTo", "RowID", "IsActive"]

        new_rows = candidates.select(
            F.lit(None).cast("long").alias("EmployeeEntraId"),
            F.col("id"),
            F.col("employeeId"),
            F.col("givenName"),
            F.col("surname"),
            F.col("userPrincipalName"),
            F.lit("0").alias("Migrated"),
            F.lit("EntraID").alias("ODTSourceSystem"),
            F.lit(source_system_id).alias("SourceSystemID"),
            F.col("IngestionDate"),
            F.lit(None).cast("string").alias("ValidTo"),
            F.col("RowID"),
            F.lit("Y").alias("IsActive"),
        )

        # Step 4: Close old versions of changed records
        LoggingUtil().log_info("Closing old versions of changed records")
        changed_ids = candidates.filter(F.col("HistoricIsActive") == "Y").select("id")
        closing_date = F.to_timestamp(F.date_sub(F.current_date(), 1))
        old_versions = (
            hrm_active
            .join(changed_ids, "id")
            .withColumn("IsActive", F.lit("N"))
            .withColumn("ValidTo", closing_date.cast("string"))
            .select(*output_cols)
        )

        # Step 5: Unchanged active records (active in hrm, not in candidates)
        candidate_ids = candidates.select("id")
        unchanged = hrm_active.join(candidate_ids, "id", "left_anti").select(*output_cols)

        # All inactive historical records stay as-is
        historical = hrm_data.filter(F.col("IsActive") == "N").select(*output_cols)

        # Step 6: Reassign EmployeeEntraId with ROW_NUMBER ordered by existing ID (nulls last for new rows)
        LoggingUtil().log_info("Reassigning EmployeeEntraId via ROW_NUMBER")
        win = Window.orderBy(F.col("EmployeeEntraId").asc_nulls_last())
        final_df = (
            new_rows
            .unionByName(old_versions)
            .unionByName(unchanged)
            .unionByName(historical)
            .withColumn("EmployeeEntraId", F.row_number().over(win))
        )

        insert_count = final_df.count()
        LoggingUtil().log_info(f"Final EntraID harmonised row count: {insert_count}")

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "entraid",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "entraid",
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
