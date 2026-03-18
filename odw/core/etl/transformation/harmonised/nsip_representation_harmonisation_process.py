from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, Tuple


# Columns used to build the MD5 RowID hash, matching the IFNULL(CAST(... AS String), '.') list
# in the original notebook's final SELECT DISTINCT
_REPRESENTATION_ROW_ID_COLUMNS = [
    "NSIPRepresentaionID",
    "representationId",
    "referenceId",
    "examinationLibraryRef",
    "caseRef",
    "caseId",
    "status",
    "originalRepresentation",
    "redacted",
    "redactedRepresentation",
    "redactedBy",
    "redactedNotes",
    "representationFrom",
    "representedId",
    "representativeId",
    "registerFor",
    "representationType",
    "dateReceived",
    "attachmentIds",
    # Horizon-only fields
    "caseuniqueid",
    "organisationname",
    "jobtitle",
    "fullname",
    "phonenumber",
    "emailaddress",
    "buildingnumber",
    "street",
    "town",
    "county",
    "country",
    "postcode",
    "attendprelimmeeting",
    "AgentFullName",
    "AgentOrganisationName",
    "agent_phonenumber",
    "agent_emailaddress",
    "AgentBuildingNumber",
    "AgentStreet",
    "AgentTown",
    "AgentCounty",
    "AgentCountry",
    "AgentPostcode",
    "representatcompacqhearing",
    "representatissuehearing",
    "representatopenfloorhearing",
    "submitlaterreps",
    "webreference",
    "owneroroccupier",
    "powertosell",
    "entitledtoclaim",
    "other",
    "descriptionifother",
    "preferredcontactmethod",
]


class NsipRepresentationHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Representation data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-representation-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_representation"
    HORIZON_TABLE = "odw_standardised_db.horizon_nsip_relevant_representation"
    SOURCE_SYSTEM_TABLE = "odw_harmonised_db.main_sourcesystem_fact"
    OUTPUT_TABLE = "odw_harmonised_db.nsip_representation"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-representation-harmonised"

    # ------------------------------------------------------------------
    # load_data – all reads happen here
    # ------------------------------------------------------------------

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load data from:
        - Service bus harmonised table (sb_nsip_representation)
        - Horizon standardised table (horizon_nsip_relevant_representation)
        - Source system fact table (main_sourcesystem_fact)
        - Distinct representationIds from service bus (for Migrated flag)
        """
        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE}")
        horizon_data = self._load_horizon_data()

        LoggingUtil().log_info(f"Loading source system data from {self.SOURCE_SYSTEM_TABLE}")
        source_system_data = self._load_source_system_data()

        sb_representation_ids = self._load_service_bus_representation_ids()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
            "source_system_data": source_system_data,
            "sb_representation_ids": sb_representation_ids,
        }

    def _load_service_bus_data(self) -> DataFrame:
        """
        Get data out of the service bus with additional NULL fields for Horizon-only columns.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                NSIPRepresentaionID
                ,representationId
                ,referenceId
                ,examinationLibraryRef
                ,caseRef
                ,caseId
                ,status
                ,originalRepresentation
                ,redacted
                ,redactedRepresentation
                ,redactedBy
                ,redactedNotes
                ,representationFrom
                ,representedId
                ,representativeId
                ,registerFor
                ,representationType
                ,dateReceived
                ,attachmentIds

                ,CAST(NULL AS String) AS caseuniqueid
                ,CAST(NULL AS String) AS organisationname
                ,CAST(NULL AS String) AS jobtitle
                ,CAST(NULL AS String) AS fullname
                ,CAST(NULL AS String) AS phonenumber
                ,CAST(NULL AS String) AS emailaddress
                ,CAST(NULL AS String) AS buildingnumber
                ,CAST(NULL AS String) AS street
                ,CAST(NULL AS String) AS town
                ,CAST(NULL AS String) AS county
                ,CAST(NULL AS String) AS country
                ,CAST(NULL AS String) AS postcode
                ,CAST(NULL AS String) AS attendprelimmeeting
                ,CAST(NULL AS String) AS AgentFullName
                ,CAST(NULL AS String) AS AgentOrganisationName
                ,CAST(NULL AS String) AS agent_phonenumber
                ,CAST(NULL AS String) AS agent_emailaddress
                ,CAST(NULL AS String) AS AgentBuildingNumber
                ,CAST(NULL AS String) AS AgentStreet
                ,CAST(NULL AS String) AS AgentTown
                ,CAST(NULL AS String) AS AgentCounty
                ,CAST(NULL AS String) AS AgentCountry
                ,CAST(NULL AS String) AS AgentPostcode
                ,CAST(NULL AS String) AS representatcompacqhearing
                ,CAST(NULL AS String) AS representatissuehearing
                ,CAST(NULL AS String) AS representatopenfloorhearing
                ,CAST(NULL AS String) AS submitlaterreps
                ,CAST(NULL AS String) AS webreference
                ,CAST(NULL AS String) AS owneroroccupier
                ,CAST(NULL AS String) AS powertosell
                ,CAST(NULL AS String) AS entitledtoclaim
                ,CAST(NULL AS String) AS other
                ,CAST(NULL AS String) AS descriptionifother
                ,CAST(NULL AS String) AS preferredcontactmethod

                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
                ,ValidTo
                ,'' AS RowID
                ,IsActive
            FROM
                {self.SERVICE_BUS_TABLE}
        """)

    def _load_horizon_data(self) -> DataFrame:
        """
        Read Horizon representation data, filtered to latest ingested_datetime,
        non-null caseReference, and non-zero casenodeid.
        No joins are applied here – the source_system join happens in process().
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                relevantrepid
                ,casereference
                ,casenodeid
                ,RelevantRepStatus
                ,representationoriginal
                ,representationredacted
                ,redactedBy
                ,notes
                ,RelRepOnBehalfOf
                ,contactId
                ,agentcontactid
                ,RelRepOrganisation
                ,dateReceived
                ,attachmentId
                ,caseuniqueid
                ,organisationname
                ,jobtitle
                ,fullname
                ,phonenumber
                ,emailaddress
                ,buildingnumber
                ,street
                ,town
                ,county
                ,country
                ,postcode
                ,attendprelimmeeting
                ,agent_fullname
                ,agent_organisationname
                ,agent_phonenumber
                ,agent_emailaddress
                ,agent_buildingnumber
                ,agent_street
                ,agent_town
                ,Agent_County
                ,Agent_Country
                ,agent_postcode
                ,representatcompacqhearing
                ,representatissuehearing
                ,representatopenfloorhearing
                ,submitlaterreps
                ,webreference
                ,owneroroccupier
                ,powertosell
                ,entitledtoclaim
                ,other
                ,descriptionifother
                ,preferredcontactmethod
                ,IngestionDate
                ,ValidTo
                ,IsActive
            FROM
                {self.HORIZON_TABLE}
            WHERE
                ingested_datetime = (SELECT MAX(ingested_datetime) FROM {self.HORIZON_TABLE})
                AND caseReference IS NOT NULL
                AND casenodeid != 0
        """)

    def _load_source_system_data(self) -> DataFrame:
        """
        Read source system fact data for Casework.
        This is joined to Horizon data in process().
        """
        return self.spark.sql(f"""
            SELECT SourceSystemID
            FROM {self.SOURCE_SYSTEM_TABLE}
            WHERE Description = 'Casework'
                AND IsActive = 'Y'
        """)

    def _load_service_bus_representation_ids(self) -> DataFrame:
        """
        Load distinct representationIds from the service bus table.
        Used to derive the Migrated flag.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT representationId
            FROM {self.SERVICE_BUS_TABLE}
        """)

    # ------------------------------------------------------------------
    # process – pure transformation, no reads or writes
    # ------------------------------------------------------------------

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Combine service bus and Horizon data, aggregate Horizon attachmentIds,
        compute IsActive/ValidTo/Migrated/RowID, deduplicate, and return data_to_write.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)
        source_system_data: DataFrame = self.load_parameter("source_system_data", source_data)
        sb_representation_ids: DataFrame = self.load_parameter("sb_representation_ids", source_data)

        # Step 1: Join Horizon with source_system_fact and align to SB schema
        LoggingUtil().log_info("Joining Horizon with source system data and aligning to SB schema")
        horizon_joined = (
            horizon_data.alias("Horizon")
            .crossJoin(source_system_data.alias("source"))
            .select(
                F.lit(None).cast("long").alias("NSIPRepresentaionID"),
                F.col("Horizon.relevantrepid").cast("integer").alias("representationId"),
                F.concat(
                    F.coalesce(F.col("Horizon.casereference"), F.lit("")),
                    F.lit("-"),
                    F.coalesce(F.col("Horizon.relevantrepid"), F.lit("")),
                ).alias("referenceId"),
                F.lit(None).cast("string").alias("examinationLibraryRef"),
                F.col("Horizon.casereference").alias("caseRef"),
                F.col("Horizon.casenodeid").cast("integer").alias("caseId"),
                F.col("Horizon.RelevantRepStatus").alias("status"),
                F.col("Horizon.representationoriginal").alias("originalRepresentation"),
                F.col("Horizon.representationredacted").isNotNull().cast("boolean").alias("redacted"),
                F.col("Horizon.representationredacted").alias("redactedRepresentation"),
                F.col("Horizon.redactedBy"),
                F.col("Horizon.notes").alias("redactedNotes"),
                F.col("Horizon.RelRepOnBehalfOf").alias("representationFrom"),
                F.col("Horizon.contactId").cast("string").alias("representedId"),
                F.col("Horizon.agentcontactid").cast("string").alias("representativeId"),
                F.col("Horizon.RelRepOnBehalfOf").alias("registerFor"),
                F.col("Horizon.RelRepOrganisation").alias("representationType"),
                F.col("Horizon.dateReceived"),
                F.col("Horizon.attachmentId").alias("attachmentIds"),
                # Horizon-only fields
                F.col("Horizon.caseuniqueid"),
                F.col("Horizon.organisationname"),
                F.col("Horizon.jobtitle"),
                F.col("Horizon.fullname"),
                F.col("Horizon.phonenumber"),
                F.col("Horizon.emailaddress"),
                F.col("Horizon.buildingnumber"),
                F.col("Horizon.street"),
                F.col("Horizon.town"),
                F.col("Horizon.county"),
                F.col("Horizon.country"),
                F.col("Horizon.postcode"),
                F.col("Horizon.attendprelimmeeting"),
                F.col("Horizon.agent_fullname").alias("AgentFullName"),
                F.col("Horizon.agent_organisationname").alias("AgentOrganisationName"),
                F.col("Horizon.agent_phonenumber"),
                F.col("Horizon.agent_emailaddress"),
                F.col("Horizon.agent_buildingnumber").alias("AgentBuildingNumber"),
                F.col("Horizon.agent_street").alias("AgentStreet"),
                F.col("Horizon.agent_town").alias("AgentTown"),
                F.col("Horizon.Agent_County").alias("AgentCounty"),
                F.col("Horizon.Agent_Country").alias("AgentCountry"),
                F.col("Horizon.agent_postcode").alias("AgentPostcode"),
                F.col("Horizon.representatcompacqhearing"),
                F.col("Horizon.representatissuehearing"),
                F.col("Horizon.representatopenfloorhearing"),
                F.col("Horizon.submitlaterreps"),
                F.col("Horizon.webreference"),
                F.col("Horizon.owneroroccupier"),
                F.col("Horizon.powertosell"),
                F.col("Horizon.entitledtoclaim"),
                F.col("Horizon.other"),
                F.col("Horizon.descriptionifother"),
                F.col("Horizon.preferredcontactmethod"),
                # Metadata
                F.lit(0).alias("Migrated"),
                F.lit("Horizon").alias("ODTSourceSystem"),
                F.col("source.SourceSystemID"),
                F.col("Horizon.IngestionDate"),
                F.col("Horizon.ValidTo"),
                F.lit("").alias("RowID"),
                F.col("Horizon.IsActive"),
            )
            .distinct()
        )

        # Step 2: Align Horizon columns to SB and union
        LoggingUtil().log_info(f"Combining data for {self.OUTPUT_TABLE}")
        horizon_joined = horizon_joined.select(service_bus_data.columns)
        combined = service_bus_data.union(horizon_joined)

        # Step 3: Window-function calculations
        win_per_rep_desc = Window.partitionBy("representationId").orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col("representationId").asc())

        combined = (
            combined.withColumn("ReverseOrderProcessed", F.row_number().over(win_per_rep_desc))
            .withColumn("NSIPRepresentaionID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_per_rep_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        # Step 4: Compute ValidTo via self-join on (representationId, ReverseOrderProcessed - 1)
        current = combined.alias("CurrentRow")
        next_row = combined.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col("CurrentRow.representationId") == F.col("NextRow.representationId"))
            & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.NSIPRepresentaionID").alias("NSIPRepresentaionID"),
            F.col("CurrentRow.representationId").alias("representationId"),
            F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.col("CurrentRow.IsActive").alias("IsActive"),
        )

        # Step 5: Derive Migrated flag (1 if representationId exists in SB, else 0)
        sb_ids = sb_representation_ids.withColumnRenamed("representationId", "sb_representationId")
        calcs = (
            calcs.join(sb_ids, calcs["representationId"] == sb_ids["sb_representationId"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_representationId").isNotNull(), F.lit("1")).otherwise(F.lit("0")))
            .drop("sb_representationId")
        )

        # Step 6: Compute RowID via MD5 hash
        row_id_expr = F.md5(F.concat(*[F.coalesce(F.col(c).cast("string"), F.lit(".")) for c in _REPRESENTATION_ROW_ID_COLUMNS]))

        # Step 7: Rejoin calculations back onto the combined dataset
        all_columns = [c for c in combined.columns if c != "ReverseOrderProcessed"]
        columns = all_columns
        base = combined.select(all_columns).dropDuplicates()
        base = base.drop("NSIPRepresentaionID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.select(
            F.col("representationId").alias("calc_representationId"),
            F.col("IngestionDate").alias("calc_IngestionDate"),
            F.col("NSIPRepresentaionID"),
            F.col("ValidTo"),
            F.col("Migrated"),
            F.col("IsActive"),
        )

        joined = base.join(
            calcs_renamed,
            (base["representationId"] == calcs_renamed["calc_representationId"]) & (base["IngestionDate"] == calcs_renamed["calc_IngestionDate"]),
        ).select(columns)

        # Step 8: Apply RowID, set Migrated to 0 (matching notebook final SELECT), drop duplicates
        final_df = joined.withColumn("RowID", row_id_expr)
        final_df = final_df.dropDuplicates()

        insert_count = final_df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "nsip_representation",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "nsip_representation",
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
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
