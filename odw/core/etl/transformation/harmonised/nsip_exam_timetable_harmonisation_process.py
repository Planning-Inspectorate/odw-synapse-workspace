from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


_EXAM_TIMETABLE_ROW_ID_COLUMNS = [
    "NSIPExaminationTimetableID",
    "caseReference",
    "published",
    "eventId",
    "type",
    "eventTitle",
    "eventTitleWelsh",
    "description",
    "descriptionWelsh",
    "eventDate",
    "eventDeadlineStartDate",
    "Migrated",
    "ODTSourceSystem",
    "SourceSystemID",
    "IngestionDate",
    "ValidTo",
]


class NsipExamTimetableHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Exam Timetable data from service bus and Horizon sources.

    # Example usage via py_etl_executor

    ```
    input_arguments = {
        "etl_process_name": "nsip-exam-timetable-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_exam_timetable"
    HORIZON_TABLE = "odw_standardised_db.horizon_examination_timetable"
    HORIZON_NSIP_DATA_TABLE = "odw_standardised_db.horizon_nsip_data"
    OUTPUT_TABLE = "nsip_exam_timetable"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-exam-timetable-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE}")
        horizon_data = self._load_horizon_data()

        LoggingUtil().log_info(f"Loading Horizon NSIP data from {self.HORIZON_NSIP_DATA_TABLE}")
        horizon_nsip_data = self._load_horizon_nsip_data()

        sb_case_references = self._load_service_bus_case_references()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
            "horizon_nsip_data": horizon_nsip_data,
            "sb_case_references": sb_case_references,
        }

    def _load_service_bus_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                NSIPExaminationTimetableID
                ,caseReference
                ,published
                ,events
                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
                ,NULLIF(ValidTo, '') AS ValidTo
                ,'' as RowID
                ,IsActive
            FROM
                {self.SERVICE_BUS_TABLE} AS SBT
        """)

    def _load_horizon_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                CaseReference
                ,CAST(ID AS Integer) AS eventId
                ,typeofexamination AS type
                ,Name AS eventTitle
                ,NameWelsh AS eventTitleWelsh
                ,Description AS description
                ,DescriptionWelsh AS descriptionWelsh
                ,DATE_FORMAT(TO_TIMESTAMP(Date), 'yyyy-MM-dd HH:mm') AS eventDate
                ,DATE_FORMAT(TO_TIMESTAMP(DeadlineStartDateTime), 'yyyy-MM-dd HH:mm') AS eventDeadlineStartDate
                ,expected_from
            FROM
                {self.HORIZON_TABLE}
            WHERE ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_TABLE})
        """)

    def _load_horizon_nsip_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                CaseReference
                ,ExamTimetablePublishStatus
            FROM
                {self.HORIZON_NSIP_DATA_TABLE}
            WHERE ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_NSIP_DATA_TABLE})
        """)

    def _load_service_bus_case_references(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT caseReference
            FROM {self.SERVICE_BUS_TABLE}
        """)

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)
        horizon_nsip_data: DataFrame = self.load_parameter("horizon_nsip_data", source_data)
        sb_case_references: DataFrame = self.load_parameter("sb_case_references", source_data)

        # Step 1: Explode service bus events into individual rows
        LoggingUtil().log_info("Exploding service bus events into individual rows")
        service_bus_event_data = service_bus_data.select(
            F.col("NSIPExaminationTimetableID"),
            F.col("caseReference"),
            F.col("published"),
            F.explode(F.col("events")).alias("event"),
            F.col("Migrated"),
            F.col("ODTSourceSystem"),
            F.col("SourceSystemID"),
            F.col("IngestionDate"),
            F.col("ValidTo"),
            F.col("RowID"),
            F.col("IsActive"),
        ).select(
            F.col("NSIPExaminationTimetableID"),
            F.col("caseReference"),
            F.col("published"),
            F.col("event.eventId").cast("int").alias("eventId"),
            F.col("event.type").alias("type"),
            F.col("event.eventTitle").alias("eventTitle"),
            F.when(F.trim(F.col("event.eventTitleWelsh")) == "", F.lit(None)).otherwise(F.col("event.eventTitleWelsh")).alias("eventTitleWelsh"),
            F.when(F.trim(F.col("event.description")) == "", F.lit(None)).otherwise(F.col("event.description")).alias("description"),
            F.when(F.trim(F.col("event.descriptionWelsh")) == "", F.lit(None)).otherwise(F.col("event.descriptionWelsh")).alias("descriptionWelsh"),
            F.date_format(F.to_timestamp(F.col("event.date")), "yyyy-MM-dd HH:mm").alias("eventDate"),
            F.date_format(F.to_timestamp(F.col("event.eventDeadlineStartDate")), "yyyy-MM-dd HH:mm").alias("eventDeadlineStartDate"),
            F.col("Migrated"),
            F.col("ODTSourceSystem"),
            F.col("SourceSystemID"),
            F.col("IngestionDate"),
            F.col("ValidTo"),
            F.col("RowID"),
            F.col("IsActive"),
        )

        # Step 2: Join Horizon with horizon_nsip_data and align to SB schema
        LoggingUtil().log_info("Joining Horizon with NSIP data and aligning to SB schema")
        horizon_joined = (
            horizon_data.alias("Horizon")
            .join(
                horizon_nsip_data.alias("hnd"),
                F.col("Horizon.CaseReference") == F.col("hnd.CaseReference"),
                "left",
            )
            .select(
                F.lit(None).cast("long").alias("NSIPExaminationTimetableID"),
                F.col("Horizon.CaseReference").alias("caseReference"),
                F.when(F.col("hnd.ExamTimetablePublishStatus") == "Published", F.lit(True))
                .when(F.col("hnd.ExamTimetablePublishStatus") == "Not Published", F.lit(False))
                .when(F.col("hnd.ExamTimetablePublishStatus") == "Ready to Publish", F.lit(False))
                .otherwise(F.lit(None).cast("boolean"))
                .alias("published"),
                F.col("Horizon.eventId"),
                F.col("Horizon.type"),
                F.col("Horizon.eventTitle"),
                F.col("Horizon.eventTitleWelsh"),
                F.col("Horizon.description"),
                F.col("Horizon.descriptionWelsh"),
                F.col("Horizon.eventDate"),
                F.col("Horizon.eventDeadlineStartDate"),
                F.lit("0").alias("Migrated"),
                F.lit("Horizon").alias("ODTSourceSystem"),
                F.lit(None).cast("string").alias("SourceSystemID"),
                F.to_timestamp(F.col("Horizon.expected_from")).alias("IngestionDate"),
                F.lit(None).cast("string").alias("ValidTo"),
                F.lit("").alias("RowID"),
                F.lit("Y").alias("IsActive"),
            )
            .distinct()
        )

        # Step 3: Union SB + Horizon
        LoggingUtil().log_info(f"Combining data for odw_harmonised_db.{self.OUTPUT_TABLE}")
        combined = service_bus_event_data.unionByName(horizon_joined, allowMissingColumns=True)

        # Step 4: Window-function calculations
        win_per_case_desc = Window.partitionBy("caseReference").orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col("caseReference").asc())

        combined = (
            combined.withColumn("ReverseOrderProcessed", F.row_number().over(win_per_case_desc))
            .withColumn("NSIPExaminationTimetableID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_per_case_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        # Step 5: Compute ValidTo via self-join
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col("CurrentRow.caseReference") == F.col("NextRow.caseReference"))
            & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.NSIPExaminationTimetableID").alias("NSIPExaminationTimetableID"),
            F.col("CurrentRow.caseReference").alias("caseReference"),
            F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.col("CurrentRow.IsActive").alias("IsActive"),
        )

        # Step 6: Derive Migrated flag
        sb_refs = sb_case_references.withColumnRenamed("caseReference", "sb_caseReference")
        calcs = (
            calcs.join(sb_refs, calcs["caseReference"] == sb_refs["sb_caseReference"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_caseReference").isNotNull(), F.lit("1")).otherwise(F.lit("0")))
            .drop("sb_caseReference")
        )

        # Step 7: Compute RowID via MD5 hash over individual event columns
        row_id_expr = F.md5(
            F.concat_ws(
                "|",
                F.col("NSIPExaminationTimetableID").cast("string"),
                F.col("caseReference").cast("string"),
                F.col("published").cast("string"),
                F.col("eventId").cast("string"),
                F.col("type").cast("string"),
                F.col("eventTitle").cast("string"),
                F.col("eventTitleWelsh").cast("string"),
                F.col("description").cast("string"),
                F.col("descriptionWelsh").cast("string"),
                F.col("eventDate").cast("string"),
                F.col("eventDeadlineStartDate").cast("string"),
                F.col("Migrated").cast("string"),
                F.col("ODTSourceSystem").cast("string"),
                F.col("SourceSystemID").cast("string"),
                F.col("IngestionDate").cast("string"),
                F.col("ValidTo").cast("string"),
            )
        )

        # Step 8: Rejoin calculations back onto the combined dataset
        all_columns = [c for c in combined.columns if c not in {"ReverseOrderProcessed"}]
        columns = all_columns
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col("NSIPExaminationTimetableID").alias("calc_NSIPExaminationTimetableID"),
            F.col("ValidTo"),
            F.col("Migrated"),
            F.col("IsActive"),
        )

        joined = (
            base.join(
                calcs_renamed,
                base["NSIPExaminationTimetableID"] == calcs_renamed["calc_NSIPExaminationTimetableID"],
            )
            .drop("calc_NSIPExaminationTimetableID")
            .select(columns)
        )

        final_df = joined.withColumn("RowID", row_id_expr)
        final_df = final_df.dropDuplicates()

        insert_count = final_df.count()

        data_to_write = {
            f"odw_harmonised_db.{self.OUTPUT_TABLE}": {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": self.OUTPUT_TABLE,
                "file_format": "delta",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
                "partition_by": ["IsActive"],
            }
        }

        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=f"odw_harmonised_db.{self.OUTPUT_TABLE}",
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
