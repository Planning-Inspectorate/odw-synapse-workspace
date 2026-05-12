from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from datetime import datetime
from typing import Dict, Tuple

class DartApiCuratedProcess(CurationProcess):
    """
    Curated ETL process to ingest appeal_has and appeal_s78
    into a single curated table: odw_curated_db.dart_api

    Direct conversion from PySpark notebook.
    """
    OUTPUT_TABLE = "odw_curated_db.dart_api"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "dart-api-curated"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info("Loading harmonised appeal_has data")

        appeal_has_df = self.spark.sql("""
            SELECT
                h.caseId,
                h.caseReference,
                h.caseStatus,
                h.caseType,
                h.caseProcedure,
                h.lpaCode,
                l.lpaName,
                h.allocationLevel,
                h.allocationBand,
                h.caseSpecialisms,
                h.caseSubmittedDate,
                h.caseCreatedDate,
                h.caseUpdatedDate,
                h.caseValidDate,
                h.caseValidationDate,
                h.caseValidationOutcome,
                h.caseValidationInvalidDetails,
                h.caseValidationIncompleteDetails,
                h.caseExtensionDate,
                h.caseStartedDate,
                h.casePublishedDate,
                h.linkedCaseStatus,
                h.leadCaseReference,
                h.caseWithdrawnDate,
                h.caseTransferredDate,
                h.transferredCaseClosedDate,
                h.caseDecisionOutcomeDate,
                h.caseDecisionPublishedDate,
                h.caseDecisionOutcome,
                h.caseCompletedDate,
                h.enforcementNotice,
                h.applicationReference,
                h.applicationDate,
                h.applicationDecision,
                h.applicationDecisionDate AS lpaDecisionDate,
                h.caseSubmissionDueDate,
                h.siteAddressLine1,
                h.siteAddressLine2,
                h.siteAddressTown,
                h.siteAddressCounty,
                h.siteAddressPostcode,
                h.isCorrectAppealType,
                h.originalDevelopmentDescription,
                h.changedDevelopmentDescription,
                h.newConditionDetails,
                h.nearbyCaseReferences,
                h.neighbouringSiteAddresses,
                h.affectedListedBuildingNumbers,
                h.appellantCostsAppliedFor,
                h.lpaCostsAppliedFor,
                CONCAT(su.firstName, ' ', su.lastName) AS appellantName,
                e.eventType AS typeOfEvent,
                e.eventStartDateTime AS startDateOfTheEvent,
                CONCAT(ent_ins.givenName, ' ', ent_ins.surname) AS inspectorName,
                CONCAT(ent_co.givenName, ' ', ent_co.surname) AS caseOfficerName,
                i.qualifications AS inspectorQualifications
            FROM odw_harmonised_db.appeal_has h
                LEFT JOIN odw_harmonised_db.pins_lpa l
                    ON h.lpaCode = l.pinsLpaCode AND l.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.sb_service_user su
                    ON h.caseReference = su.caseReference
                   AND su.serviceUserType = 'Appellant'
                   AND su.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.sb_appeal_event e
                    ON h.caseReference = e.caseReference AND e.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.entraid ent_ins
                    ON h.inspectorId = ent_ins.id AND ent_ins.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.entraid ent_co
                    ON h.caseOfficerId = ent_co.id AND ent_co.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.horizon_pins_inspector i
                    ON UPPER(ent_ins.userPrincipalName) = UPPER(i.email)
                   AND i.isActive = 'Y'
            WHERE
                h.IsActive = 'Y'
                AND h.lpaCode <> 'Q9999'
                AND h.caseReference > '6000000'
                AND h.ODTSourceSystem = 'ODT'
        """)

        LoggingUtil().log_info("Loading harmonised appeal_s78 data")

        appeal_s78_df = self.spark.sql("""
            SELECT
                h.caseId,
                h.caseReference,
                h.caseStatus,
                h.caseType,
                h.caseProcedure,
                h.lpaCode,
                l.lpaName,
                h.allocationLevel,
                h.allocationBand,
                h.caseSpecialisms,
                h.caseSubmittedDate,
                h.caseCreatedDate,
                h.caseUpdatedDate,
                h.caseValidDate,
                h.caseValidationDate,
                h.caseValidationOutcome,
                h.caseValidationInvalidDetails,
                h.caseValidationIncompleteDetails,
                h.caseExtensionDate,
                h.caseStartedDate,
                h.casePublishedDate,
                h.linkedCaseStatus,
                h.leadCaseReference,
                h.caseWithdrawnDate,
                h.caseTransferredDate,
                h.transferredCaseClosedDate,
                h.caseDecisionOutcomeDate,
                h.caseDecisionPublishedDate,
                h.caseDecisionOutcome,
                h.caseCompletedDate,
                h.enforcementNotice,
                h.applicationReference,
                h.applicationDate,
                h.applicationDecision,
                h.applicationDecisionDate AS lpaDecisionDate,
                h.caseSubmissionDueDate,
                h.siteAddressLine1,
                h.siteAddressLine2,
                h.siteAddressTown,
                h.siteAddressCounty,
                h.siteAddressPostcode,
                h.isCorrectAppealType,
                h.originalDevelopmentDescription,
                h.changedDevelopmentDescription,
                h.newConditionDetails,
                h.nearbyCaseReferences,
                h.neighbouringSiteAddresses,
                h.affectedListedBuildingNumbers,
                h.appellantCostsAppliedFor,
                h.lpaCostsAppliedFor,
                CONCAT(su.firstName, ' ', su.lastName) AS appellantName,
                e.eventType AS typeOfEvent,
                e.eventStartDateTime AS startDateOfTheEvent,
                CONCAT(ent_ins.givenName, ' ', ent_ins.surname) AS inspectorName,
                CONCAT(ent_co.givenName, ' ', ent_co.surname) AS caseOfficerName,
                i.qualifications AS inspectorQualifications
            FROM odw_harmonised_db.appeal_s78 h
                LEFT JOIN odw_harmonised_db.pins_lpa l
                    ON h.lpaCode = l.pinsLpaCode AND l.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.sb_service_user su
                    ON h.caseReference = su.caseReference
                   AND su.serviceUserType = 'Appellant'
                   AND su.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.sb_appeal_event e
                    ON h.caseReference = e.caseReference AND e.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.entraid ent_ins
                    ON h.inspectorId = ent_ins.id AND ent_ins.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.entraid ent_co
                    ON h.caseOfficerId = ent_co.id AND ent_co.isActive = 'Y'
                LEFT JOIN odw_harmonised_db.horizon_pins_inspector i
                    ON UPPER(ent_ins.userPrincipalName) = UPPER(i.email)
                   AND i.isActive = 'Y'
            WHERE
                h.IsActive = 'Y'
                AND h.lpaCode <> 'Q9999'
                AND h.caseReference > '6000000'
                AND h.ODTSourceSystem = 'ODT'
        """)

        return {
            "appeal_has": appeal_has_df,
            "appeal_s78": appeal_s78_df
        }

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        appeal_has_df = self.load_parameter("appeal_has", source_data)
        appeal_s78_df = self.load_parameter("appeal_s78", source_data)

        # UNION
        df = appeal_has_df.unionByName(appeal_s78_df)

        # Clean applicationReference
        clean = F.col("applicationReference").cast("string")
        clean = F.regexp_replace(clean, r"[.]+\s*$", "")
        clean = F.regexp_replace(clean, r"\s+$", "")

        df = df.withColumn(
            "applicationReference",
            F.when(clean.isNull() | (F.trim(clean) == ""), F.lit("UNKNOWN"))
             .otherwise(clean)
        )

        # Boolean normalisation
        for c in [
            "enforcementNotice",
            "isCorrectAppealType",
            "appellantCostsAppliedFor",
            "lpaCostsAppliedFor",
            "changedDevelopmentDescription",
        ]:
            if c in df.columns:
                df = df.withColumn(
                    c,
                    F.when(F.upper(F.col(c)).isin("Y", "YES", "TRUE", "T", "1"), True)
                     .when(F.upper(F.col(c)).isin("N", "NO", "FALSE", "F", "0"), False)
                     .otherwise(F.lit(None).cast("boolean"))
                )

        # Deduplicate
        df = df.dropDuplicates(["caseReference", "applicationReference"])

        # Partition & sort
        self.spark.conf.set("spark.sql.shuffle.partitions", "128")
        df = df.repartitionByRange("applicationReference") \
               .sortWithinPartitions("caseReference")

        # Validation
        bad_cnt = df.filter(
            F.col("applicationReference").rlike(r"[.]\s*$") |
            F.col("applicationReference").rlike(r"\s+$") |
            F.col("applicationReference").isNull() |
            (F.trim(F.col("applicationReference")) == "")
        ).count()

        if bad_cnt != 0:
            raise ValueError(f"Found {bad_cnt} bad applicationReference values")

        # Persist & count
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        insert_count = df.count()

        LoggingUtil().log_info(f"DART API curated row count: {insert_count}")

        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": "dart_api",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": "dart_api",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {
                    "overwriteSchema": "true"
                },
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
