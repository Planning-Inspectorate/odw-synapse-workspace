from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple


class NsipExamTimetableCuratedProcess(CurationProcess):
    """
    ETL process for curating NSIP Exam Timetable data from the harmonised layer.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-exam-timetable-curated",
        "debug": False
    }
    ```
    """

    HARMONISED_TABLE = "odw_harmonised_db.nsip_exam_timetable"
    CURATED_PROJECT_TABLE = "odw_curated_db.nsip_project"
    OUTPUT_TABLE = "odw_curated_db.nsip_exam_timetable"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-exam-timetable-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load source data, selecting only the columns needed downstream.
        No joins or transformations are applied here – only reads.
        """
        LoggingUtil().log_info(f"Loading harmonised NSIP Exam Timetable data from {self.HARMONISED_TABLE}")
        harmonised_exam_timetable = self.spark.sql(f"""
            SELECT
                caseReference,
                published,
                events,
                IngestionDate,
                ODTSourceSystem
            FROM {self.HARMONISED_TABLE}
        """)

        LoggingUtil().log_info(f"Loading curated NSIP Project data from {self.CURATED_PROJECT_TABLE}")
        curated_projects = self.spark.sql(f"""
            SELECT caseReference
            FROM {self.CURATED_PROJECT_TABLE}
        """)

        return {
            "harmonised_exam_timetable": harmonised_exam_timetable,
            "curated_projects": curated_projects,
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Apply curated transformations to the loaded source data.

        Legacy behaviour for nsip_exam_timetable curated is:
        - keep Horizon records only
        - for each caseReference, keep the latest Horizon IngestionDate
        - inner join to curated nsip_project
        """
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        harmonised_exam_timetable: DataFrame = self.load_parameter("harmonised_exam_timetable", source_data)
        curated_projects: DataFrame = self.load_parameter("curated_projects", source_data)

        # Match legacy logic: only Horizon records are eligible
        horizon_exam_timetable = harmonised_exam_timetable.filter(F.lower(F.col("ODTSourceSystem")) == "horizon")

        # Match legacy logic: latest Horizon record per caseReference
        latest_horizon_dates = horizon_exam_timetable.groupBy("caseReference").agg(F.max("IngestionDate").alias("latest_date"))

        latest_horizon_exam_timetable = horizon_exam_timetable.join(
            latest_horizon_dates,
            (horizon_exam_timetable["caseReference"] == latest_horizon_dates["caseReference"])
            & (horizon_exam_timetable["IngestionDate"] == latest_horizon_dates["latest_date"]),
            "inner",
        ).select(horizon_exam_timetable["*"])

        # Match legacy logic: only caseReferences that exist in curated nsip_project
        df = (
            latest_horizon_exam_timetable.join(
                curated_projects,
                latest_horizon_exam_timetable["caseReference"] == curated_projects["caseReference"],
                "inner",
            )
            .select(
                latest_horizon_exam_timetable["caseReference"],
                latest_horizon_exam_timetable["published"],
                latest_horizon_exam_timetable["events"],
            )
            .distinct()
        )

        insert_count = df.count()
        LoggingUtil().log_info(f"Curated NSIP Exam Timetable row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "nsip_exam_timetable",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "nsip_exam_timetable",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {},
            }
        }
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
