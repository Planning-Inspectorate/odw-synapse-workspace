<<<<<<< HEAD
from typing import Any

from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess


class ServiceUserHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "service_user"

    SERVICE_BUS_TABLE = "sb_service_user"
    HZN_SERVICE_USER_TABLE = "horizon_case_involvement"
    HZN_NSIP_PROJECT_TABLE = "horizon_nsip_data"
    HZN_NSIP_REPRESENTATION_TABLE = "horizon_nsip_relevant_representation"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark

    def get_name(self) -> str:
        return "service_user_harmonisation_process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError("ServiceUserHarmonisationProcess.load_data() has not been implemented yet.")

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError("ServiceUserHarmonisationProcess.process() has not been implemented yet.")
=======
from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Dict, List, Tuple


class ServiceUserHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising service user data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "service-user-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_service_user"
    HZN_SERVICE_USER_TABLE = "odw_standardised_db.horizon_case_involvement"
    HZN_NSIP_PROJECT_TABLE = "odw_standardised_db.horizon_nsip_data"
    HZN_NSIP_REPRESENTATION_TABLE = "odw_standardised_db.horizon_nsip_relevant_representation"
    OUTPUT_TABLE = "odw_harmonised_db.service_user"
    PRIMARY_KEY = "TEMP_PK"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "service-user-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info("Loading Horizon appeal casework appellant data")
        appeal_casework_appellant_data = self._load_appeal_casework_data("Appellant", ["Appellant", "tAppellant", "Apellant"])

        LoggingUtil().log_info("Loading Horizon appeal casework agent data")
        appeal_casework_agent_data = self._load_appeal_casework_data("Agent", ["Agent"])

        LoggingUtil().log_info("Loading Horizon NSIP project applicant data")
        nsip_project_applicant_data = self._load_nsip_project_applicant_data()

        LoggingUtil().log_info("Loading Horizon relevant representations (represented) data")
        relevant_reps_represented_data = self._load_relevant_represented_data()

        LoggingUtil().log_info("Loading Horizon relevant representations (agent) data")
        relevant_reps_agent_data = self._load_relevant_agent_data()

        sb_primary_keys = self._load_service_bus_primary_keys()

        return {
            "service_bus_data": service_bus_data,
            "appeal_casework_appellant_data": appeal_casework_appellant_data,
            "appeal_casework_agent_data": appeal_casework_agent_data,
            "nsip_project_applicant_data": nsip_project_applicant_data,
            "relevant_reps_represented_data": relevant_reps_represented_data,
            "relevant_reps_agent_data": relevant_reps_agent_data,
            "sb_primary_keys": sb_primary_keys,
        }

    def _load_service_bus_data(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(id, caseReference, serviceUserType)) AS {self.PRIMARY_KEY}
                ,ServiceUserID
                ,id
                ,salutation
                ,firstname
                ,lastname
                ,addressline1
                ,addressline2
                ,addressTown
                ,addressCounty
                ,postcode
                ,addressCountry
                ,organisation
                ,organisationType
                ,role
                ,telephoneNumber
                ,otherPhoneNumber
                ,faxNumber
                ,emailAddress
                ,webAddress
                ,serviceUserType
                ,serviceUserType AS serviceUserTypeInternal
                ,caseReference
                ,sourceSystem
                ,sourceSuid
                ,CAST(NULL AS String) AS contactMethod

                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
                ,NULLIF(ValidTo, '') AS ValidTo
                ,'' AS RowID
                ,IsActive
            FROM
                {self.SERVICE_BUS_TABLE}
        """)

    def _load_appeal_casework_data(self, service_user_type: str, involvement_types: List[str]) -> DataFrame:
        involvement_types_sql = ", ".join([f"'{v}'" for v in involvement_types])
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(CAST(ContactId AS String), ''),
                        COALESCE(CAST(case_number AS String), ''),
                        '{service_user_type}'
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS Long) AS ServiceUserID
                ,CAST(ContactId AS String) AS id
                ,CAST(Title AS String) AS salutation
                ,CAST(FirstName AS String) AS firstName
                ,CAST(LastName AS String) AS lastName
                ,CAST(Address1 AS String) AS addressLine1
                ,CAST(Address2 AS String) AS addressLine2
                ,CAST(City AS String) AS addressTown
                ,CAST(County AS String) AS addressCounty
                ,CAST(PostCode AS String) AS postcode
                ,CAST(Country AS String) AS addressCountry
                ,CAST(OrganisationName AS String) AS organisation
                ,CAST(OrganisationTypeName AS String) AS organisationType
                ,CAST(NULL AS String) AS role
                ,CAST(TelephoneOffice AS String) AS telephoneNumber
                ,CAST(TelephoneMobile AS String) AS otherPhoneNumber
                ,CAST(Fax AS String) AS faxNumber
                ,CAST(Email AS String) AS emailAddress
                ,CAST(NULL AS String) AS webAddress
                ,CAST('{service_user_type}' AS String) AS serviceUserType
                ,CAST('{service_user_type}' AS String) AS serviceUserTypeInternal
                ,CAST(case_number AS String) AS caseReference
                ,CAST('Horizon' AS String) AS sourceSystem
                ,CAST(ContactID AS String) AS sourceSuid
                ,CAST(NULL AS String) AS contactMethod

                ,'0' AS Migrated
                ,'Horizon' AS ODTSourceSystem
                ,NULL AS SourceSystemID
                ,TO_TIMESTAMP(expected_from) AS IngestionDate
                ,CAST(NULL AS String) AS ValidTo
                ,'' AS RowID
                ,'Y' AS IsActive
            FROM
                {self.HZN_SERVICE_USER_TABLE}
            WHERE
                expected_from = (SELECT MAX(expected_from) FROM {self.HZN_SERVICE_USER_TABLE})
                AND ContactId IS NOT NULL
                AND typeOfInvolvement IN ({involvement_types_sql})
        """)

    def _load_nsip_project_applicant_data(self) -> DataFrame:
        service_user_type = "Applicant"
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(CAST(caseNodeId AS String), ''),
                        COALESCE(CAST(CaseReference AS String), ''),
                        '{service_user_type}'
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS Long) AS ServiceUserID
                ,CAST(caseNodeId AS String) AS id
                ,CAST(NULL AS String) AS salutation
                ,CAST(ApplicantFirstName AS String) AS firstName
                ,CAST(ApplicantLastName AS String) AS lastName
                ,CAST(AddressLine1 AS String) AS addressLine1
                ,CAST(AddressLine2 AS String) AS addressLine2
                ,CAST(AddressTown AS String) AS addressTown
                ,CAST(AddressCounty AS String) AS addressCounty
                ,CAST(PostCode AS String) AS postcode
                ,CAST(NULL AS String) AS addressCountry
                ,CAST(PromoterName AS String) AS organisation
                ,CAST(NULL AS String) AS organisationType
                ,CAST(NULL AS String) AS role
                ,CAST(ApplicantPhoneNumber AS String) AS telephoneNumber
                ,CAST(NULL AS String) AS otherPhoneNumber
                ,CAST(NULL AS String) AS faxNumber
                ,CAST(ApplicantEmailAddress AS String) AS emailAddress
                ,CAST(ApplicantWebAddress AS String) AS webAddress
                ,CAST('{service_user_type}' AS String) AS serviceUserType
                ,CAST('{service_user_type}' AS String) AS serviceUserTypeInternal
                ,CAST(CaseReference AS String) AS caseReference
                ,CAST('Horizon' AS String) AS sourceSystem
                ,CAST(caseNodeId AS String) AS sourceSuid
                ,CAST(NULL AS String) AS contactMethod

                ,'0' AS Migrated
                ,'Horizon' AS ODTSourceSystem
                ,NULL AS SourceSystemID
                ,TO_TIMESTAMP(expected_from) AS IngestionDate
                ,CAST(NULL AS String) AS ValidTo
                ,'' AS RowID
                ,'Y' AS IsActive
            FROM
                {self.HZN_NSIP_PROJECT_TABLE}
            WHERE
                expected_from = (SELECT MAX(expected_from) FROM {self.HZN_NSIP_PROJECT_TABLE})
                AND caseNodeId IS NOT NULL
        """)

    def _load_relevant_represented_data(self) -> DataFrame:
        service_user_type = "RepresentationContact"
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(CAST(ContactId AS String), ''),
                        COALESCE(CAST(CaseReference AS String), ''),
                        '{service_user_type}'
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS Long) AS ServiceUserID
                ,CAST(ContactId AS String) AS id
                ,CAST(NULL AS String) AS salutation
                ,CAST(NULL AS String) AS firstName
                ,CAST(FullName AS String) AS lastName
                ,CAST(CONCAT(BuildingNumber, ' ', Street) AS String) AS addressLine1
                ,CAST(NULL AS String) AS addressLine2
                ,CAST(Town AS String) AS addressTown
                ,CAST(County AS String) AS addressCounty
                ,CAST(PostCode AS String) AS postcode
                ,CAST(Country AS String) AS addressCountry
                ,CAST(OrganisationName AS String) AS organisation
                ,CAST(NULL AS String) AS organisationType
                ,CAST(JobTitle AS String) AS role
                ,CAST(PhoneNumber AS String) AS telephoneNumber
                ,CAST(NULL AS String) AS otherPhoneNumber
                ,CAST(NULL AS String) AS faxNumber
                ,CAST(EmailAddress AS String) AS emailAddress
                ,CAST(NULL AS String) AS webAddress
                ,CAST('{service_user_type}' AS String) AS serviceUserType
                ,CAST('{service_user_type}_Represented' AS String) AS serviceUserTypeInternal
                ,CAST(CaseReference AS String) AS caseReference
                ,CAST('Horizon' AS String) AS sourceSystem
                ,CAST(ContactID AS String) AS sourceSuid
                ,CAST(
                    CASE
                        WHEN AgentContactId IS NULL THEN
                            CASE
                                WHEN LOWER(preferredContactMethod) = 'letter' THEN 'post'
                                WHEN LOWER(preferredContactMethod) = 'e-mail' THEN 'email'
                                ELSE LOWER(preferredContactMethod)
                            END
                        ELSE NULL
                    END AS String
                ) AS contactMethod

                ,'0' AS Migrated
                ,'Horizon' AS ODTSourceSystem
                ,NULL AS SourceSystemID
                ,TO_TIMESTAMP(expected_from) AS IngestionDate
                ,CAST(NULL AS String) AS ValidTo
                ,'' AS RowID
                ,'Y' AS IsActive
            FROM
                {self.HZN_NSIP_REPRESENTATION_TABLE}
            WHERE
                expected_from = (SELECT MAX(expected_from) FROM {self.HZN_NSIP_REPRESENTATION_TABLE})
                AND ContactID IS NOT NULL
                AND CaseReference IS NOT NULL
        """)

    def _load_relevant_agent_data(self) -> DataFrame:
        service_user_type = "RepresentationContact"
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(CAST(AgentContactId AS String), ''),
                        COALESCE(CAST(CaseReference AS String), ''),
                        '{service_user_type}'
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS Long) AS ServiceUserID
                ,CAST(AgentContactId AS String) AS id
                ,CAST(NULL AS String) AS salutation
                ,CAST(NULL AS String) AS firstName
                ,CAST(Agent_FullName AS String) AS lastName
                ,CAST(CONCAT(Agent_BuildingNumber, ' ', Agent_Street) AS String) AS addressLine1
                ,CAST(NULL AS String) AS addressLine2
                ,CAST(Agent_Town AS String) AS addressTown
                ,CAST(Agent_County AS String) AS addressCounty
                ,CAST(Agent_Postcode AS String) AS postcode
                ,CAST(Agent_Country AS String) AS addressCountry
                ,CAST(Agent_OrganisationName AS String) AS organisation
                ,CAST(NULL AS String) AS organisationType
                ,CAST(Agent_JobTitle AS String) AS role
                ,CAST(Agent_PhoneNumber AS String) AS telephoneNumber
                ,CAST(NULL AS String) AS otherPhoneNumber
                ,CAST(NULL AS String) AS faxNumber
                ,CAST(Agent_EmailAddress AS String) AS emailAddress
                ,CAST(NULL AS String) AS webAddress
                ,CAST('{service_user_type}' AS String) AS serviceUserType
                ,CAST('{service_user_type}_Agent' AS String) AS serviceUserTypeInternal
                ,CAST(CaseReference AS String) AS caseReference
                ,CAST('Horizon' AS String) AS sourceSystem
                ,CAST(ContactID AS String) AS sourceSuid
                ,CAST(
                    CASE
                        WHEN LOWER(preferredContactMethod) = 'letter' THEN 'post'
                        WHEN LOWER(preferredContactMethod) = 'e-mail' THEN 'email'
                        ELSE LOWER(preferredContactMethod)
                    END AS String
                ) AS contactMethod

                ,'0' AS Migrated
                ,'Horizon' AS ODTSourceSystem
                ,NULL AS SourceSystemID
                ,TO_TIMESTAMP(expected_from) AS IngestionDate
                ,CAST(NULL AS String) AS ValidTo
                ,'' AS RowID
                ,'Y' AS IsActive
            FROM
                {self.HZN_NSIP_REPRESENTATION_TABLE}
            WHERE
                expected_from = (SELECT MAX(expected_from) FROM {self.HZN_NSIP_REPRESENTATION_TABLE})
                AND AgentContactId IS NOT NULL
                AND CaseReference IS NOT NULL
        """)

    def _load_service_bus_primary_keys(self) -> DataFrame:
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(id, caseReference, serviceUserType)) AS {self.PRIMARY_KEY}
            FROM
                {self.SERVICE_BUS_TABLE}
        """)

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        appeal_casework_appellant_data: DataFrame = self.load_parameter("appeal_casework_appellant_data", source_data)
        appeal_casework_agent_data: DataFrame = self.load_parameter("appeal_casework_agent_data", source_data)
        nsip_project_applicant_data: DataFrame = self.load_parameter("nsip_project_applicant_data", source_data)
        relevant_reps_represented_data: DataFrame = self.load_parameter("relevant_reps_represented_data", source_data)
        relevant_reps_agent_data: DataFrame = self.load_parameter("relevant_reps_agent_data", source_data)
        sb_primary_keys: DataFrame = self.load_parameter("sb_primary_keys", source_data)

        aligned_columns = service_bus_data.columns
        appeal_casework_appellant_data = appeal_casework_appellant_data.select(aligned_columns)
        appeal_casework_agent_data = appeal_casework_agent_data.select(aligned_columns)
        nsip_project_applicant_data = nsip_project_applicant_data.select(aligned_columns)
        relevant_reps_represented_data = relevant_reps_represented_data.select(aligned_columns)
        relevant_reps_agent_data = relevant_reps_agent_data.select(aligned_columns)

        combined = (
            service_bus_data.union(appeal_casework_appellant_data)
            .union(appeal_casework_agent_data)
            .union(nsip_project_applicant_data)
            .union(relevant_reps_represented_data)
            .union(relevant_reps_agent_data)
        )

        pk = self.PRIMARY_KEY
        win_pk_desc = Window.partitionBy(pk).orderBy(F.col("IngestionDate").desc())
        win_global_asc = Window.orderBy(F.col("IngestionDate").asc(), F.col(pk).asc())

        calc_base = (
            combined.withColumn("ReverseOrderProcessed", F.row_number().over(win_pk_desc))
            .withColumn("ServiceUserID", F.row_number().over(win_global_asc))
            .withColumn(
                "IsActive",
                F.when(F.row_number().over(win_pk_desc) == 1, F.lit("Y")).otherwise(F.lit("N")),
            )
        )

        current = calc_base.alias("CurrentRow")
        next_row = calc_base.alias("NextRow")

        calcs = current.join(
            next_row,
            (F.col("CurrentRow." + pk) == F.col("NextRow." + pk))
            & (F.col("CurrentRow.ReverseOrderProcessed") - 1 == F.col("NextRow.ReverseOrderProcessed")),
            "left_outer",
        ).select(
            F.col("CurrentRow.ServiceUserID").alias("ServiceUserID"),
            F.col("CurrentRow." + pk).alias(pk),
            F.col("CurrentRow.IngestionDate").alias("IngestionDate"),
            F.coalesce(
                F.when(F.col("CurrentRow.ValidTo") == "", F.lit(None)).otherwise(F.col("CurrentRow.ValidTo")),
                F.col("NextRow.IngestionDate"),
            ).alias("ValidTo"),
            F.col("CurrentRow.IsActive").alias("IsActive"),
        )

        sb_keys = sb_primary_keys.withColumnRenamed(pk, "sb_pk")
        calcs = (
            calcs.join(sb_keys, calcs[pk] == sb_keys["sb_pk"], "left_outer")
            .withColumn("Migrated", F.when(F.col("sb_pk").isNotNull(), F.lit("1")).otherwise(F.lit("0")))
            .drop("sb_pk")
        )

        selected_columns = [
            pk,
            "ServiceUserID",
            "id",
            "salutation",
            "firstname",
            "lastname",
            "addressline1",
            "addressline2",
            "addressTown",
            "addressCounty",
            "postcode",
            "addressCountry",
            "organisation",
            "organisationType",
            "role",
            "telephoneNumber",
            "otherPhoneNumber",
            "faxNumber",
            "emailAddress",
            "webAddress",
            "serviceUserType",
            "serviceUserTypeInternal",
            "caseReference",
            "sourceSystem",
            "sourceSuid",
            "contactMethod",
            "Migrated",
            "ODTSourceSystem",
            "IngestionDate",
            "ValidTo",
            "RowID",
            "IsActive",
        ]

        results = calc_base.select(*selected_columns).dropDuplicates()
        columns = results.columns
        results = results.drop("ServiceUserID", "ValidTo", "Migrated", "IsActive")

        calcs_renamed = calcs.withColumnRenamed(pk, f"temp_{pk}").withColumnRenamed("IngestionDate", "temp_IngestionDate")

        final_df = results.join(
            calcs_renamed,
            (calcs_renamed[f"temp_{pk}"] == results[pk]) & (calcs_renamed["temp_IngestionDate"] == results["IngestionDate"]),
            "inner",
        ).select(columns)

        final_df = final_df.drop(pk).dropDuplicates()
        insert_count = final_df.count()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "service_user",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "service_user",
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
>>>>>>> 925b68494 (3 notebooks were refactored and updated etl_process_factory.py file)
