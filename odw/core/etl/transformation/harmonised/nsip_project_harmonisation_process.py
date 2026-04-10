from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.util.udf import absolute
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.errors.exceptions.captured import AnalysisException
from datetime import datetime
from typing import Dict
import json


class NSIPProjectHarmonisationProcess(HarmonisationProcess):
    """
        # Example usage

        ```
        params = {
        "entity_stage_name": "NSIP Project Harmonisation"
    }
        NSIPProjectHarmonisationProcess(spark).run(**params)
        ```
    """
    OUTPUT_TABLE = "nsip_project"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"
        self.entity = "nsip_project"
        self.service_bus_table = f"{self.hrm_db}.sb_nsip_project"
        self.horizon_table = f"{self.std_db}.horizon_nsip_data"
        self.harmonised_table = f"{self.hrm_db}.{self.entity}"

    @classmethod
    def get_name(cls):
        return "NSIP Project Harmonisation"

    def _load_service_bus_data(self):
        pass

    def _load_first_seen_service_bus_record(self):
        return self.spark.sql(
            f"""
            (   SELECT
                    caseId
                    ,MIN(IngestionDate) AS ingested
                FROM
                    {self.service_bus_table}
                GROUP BY
                    caseId
            )
            """
        )

    def _load_horizon_data(self):
        return self.spark.sql(f"""
            SELECT
                CAST(NULL AS Long) AS NSIPProjectInfoInternalID
                ,CAST(Horizon.caseNodeId AS Long) AS caseid
                ,Horizon.casereference
                ,Horizon.projectname
                ,Horizon.projectNameWelsh AS projectNameWelsh
                ,Horizon.projectDescription AS projectDescription
                ,Horizon.projectDescriptionWelsh AS projectDescriptionWelsh
                ,Horizon.summary
                ,Horizon.caseCreatedDate
                ,CAST(NULL AS String) AS decision
                ,CASE WHEN LOWER(projectstatus) = 'not published' THEN 'unpublished' ELSE LOWER(projectstatus) END AS publishStatus
                ,sector
                ,projecttype AS projectType
                ,'Horizon' AS sourceSystem
                ,stage
                ,projectLocation
                ,Horizon.projectLocationWelsh AS projectLocationWelsh
                ,projectEmailAddress
                ,LOWER(Region) AS Region
                ,CAST(transboundary AS Boolean) AS transboundary
                ,CAST(easting AS Integer) AS easting
                ,CAST(northing AS Integer) AS northing
                ,CAST(welshLanguage AS Boolean) AS welshLanguage
                ,LOWER(mapZoomLevel) AS mapZoomLevel
                ,sos AS secretaryOfState
                ,datePINSFirstNotifiedOfProject
                ,dateProjectAppearsOnWebsite
                ,anticipatedSubmissionDateNonSpecific
                ,anticipatedDateOfSubmission
                ,screeningOpinionSought
                ,screeningOpinionIssued
                ,scopingOpinionSought
                ,scopingOpinionIssued
                ,section46Notification
                ,dateOfDCOSubmission
                ,deadlineForAcceptanceDecision
                ,dateOfDCOAcceptance
                ,CAST(NULL AS String) AS dateOfNonAcceptance
                ,dateOfRepresentationPeriodOpen
                ,dateOfRelevantRepresentationClose
                ,extensionToDateRelevantRepresentationsClose
                ,dateRRepAppearOnWebsite
                ,CAST(NULL AS String) AS dateIAPIDue
                ,CAST(NULL AS String) AS rule6LetterPublishDate
                ,preliminaryMeetingStartDate
                ,CAST(NULL AS string) AS notificationDateForPMAndEventsDirectlyFollowingPM
                ,CAST(NULL AS string) AS notificationDateForEventsDeveloper
                ,dateSection58NoticeReceived
                ,confirmedStartOfExamination
                ,CAST(NULL AS String) AS rule8LetterPublishDate
                ,deadlineForCloseOfExamination
                ,dateTimeExaminationEnds
                ,stage4ExtensionToExamCloseDate
                ,deadlineForSubmissionOfRecommendation
                ,dateOfRecommendations
                ,stage5ExtensionToRecommendationDeadline
                ,deadlineForDecision
                ,confirmedDateOfDecision
                ,stage5ExtensionToDecisionDeadline
                ,jRPeriodEndDate
                ,dateProjectWithdrawn
                ,CAST(NULL AS String) AS operationsLeadId
                ,CAST(NULL AS String) AS operationsManagerId
                ,CAST(NULL AS String) AS caseManagerId
                ,CAST(NULL AS String) AS nsipOfficerIds
                ,CAST(NULL AS String) AS nsipAdministrationOfficerIds
                ,CAST(NULL AS String) AS leadInspectorId
                ,CAST(NULL AS String) AS inspectorIds
                ,CAST(NULL AS String) AS environmentalServicesOfficerId
                ,CAST(NULL AS String) AS legalOfficerId
                ,CAST(NULL AS String) AS applicantId
                ,CAST(FALSE AS Boolean) AS migrationStatus
                ,CAST(NULL AS String) AS dateOfReOpenRelevantRepresentationStart
                ,CAST(NULL AS String) AS dateOfReOpenRelevantRepresentationClose
                --Start Horizon only fields
                ,examTimetablePublishStatus
                ,twitteraccountname
                ,exasize
                ,tene
                ,promotername
                ,applicantfirstname
                ,applicantlastname
                ,addressLine1
                ,addressLine2
                ,addressTown
                ,addressCounty
                ,Postcode
                ,applicantemailaddress
                ,applicantwebaddress
                ,applicantphonenumber
                ,applicantdescriptionofproject
                ,HorizonCaseNumber
                --End Horizon only fields
                -- Additional columns-2323
                ,CAST(NULL AS string) AS inceptionMeetingDate
                ,CAST(NULL AS string) AS draftDocumentSubmissionDate
                ,CAST(NULL AS string) AS programmeDocumentSubmissionDate
                ,CAST(NULL AS string) AS estimatedScopingSubmissionDate
                ,CAST(NULL AS string) AS consultationMilestoneAdequacyDate
                ,CAST(NULL AS string) AS principalAreaDisagreementSummaryStmtSubmittedDate
                ,CAST(NULL AS string) AS policyComplianceDocumentSubmittedDate
                ,CAST(NULL AS string) AS designApproachDocumentSubmittedDate
                ,CAST(NULL AS string) AS caAndTpEvidenceSubmittedDate
                ,CAST(NULL AS string) AS caseTeamIssuedCommentsDate
                ,CAST(NULL AS string) AS fastTrackAdmissionDocumentSubmittedDate
                ,CAST(NULL AS string) AS matureOutlineControlDocumentSubmittedDate
                ,CAST(NULL AS string) AS memLastUpdated
                , CAST(NULL AS string) AS multipartyApplicationCheckDocumentSubmittedDate
                ,CAST(NULL AS string) AS programmeDocumentReviewedByEstDate
                ,CAST(NULL AS string) AS publicSectorEqualityDutySubmittedDate
                ,CAST(NULL AS string) AS statutoryConsultationPeriodEndDate
                ,CAST(NULL AS string) AS submissionOfDraftDocumentsDate
                ,CAST(NULL AS string) AS updatedProgrammeDocumentReceivedDate
                ,CAST(NULL AS string) AS courtDecisionDate
                ,CAST(NULL AS string) AS decisionChallengeSubmissionDate
                ,CAST(NULL AS String) AS courtDecisionOutcomeText
                ,CAST(NULL AS String) AS recommendation
                ,CAST(NULL AS String) AS additionalComments
                ,CAST(NULL AS String) AS caAndTpEvidence
                ,CAST(NULL AS String) AS fastTrackAdmissionDocument
                ,CAST(NULL AS String) AS meetings
                ,CAST(NULL AS String) AS multipartyApplicationCheckDocument
                ,CAST(NULL AS String) AS newMaturity
                ,CAST(NULL AS double) AS numberBand2Inspectors
                ,CAST(NULL AS double) AS numberBand3Inspectors
                ,CAST(NULL AS String) AS programmeDocumentURI
                ,CAST(NULL AS String) AS publicSectorEqualityDuty
                ,CAST(NULL AS String) AS subProjectType
                ,CAST(NULL AS String) AS tier
                ,CAST(NULL AS String) AS s61SummaryURI
                ,CAST(NULL AS boolean) AS planProcessEvidence
                ,CAST(NULL AS String) AS issuesTracker
                ,CAST(NULL AS boolean) AS essentialFastTrackComponents
                ,CAST(NULL AS String) AS principalAreaDisagreementSummaryStmt
                ,CAST(NULL AS String) AS policyComplianceDocument
                ,CAST(NULL AS String) AS designApproachDocument
                ,CAST(NULL AS String) AS matureOutlineControlDocument
                ,CAST(NULL AS String) AS invoices
                ,CAST(NULL AS String) AS operationsLeadIds
                ,CAST(NULL AS String) AS operationsManagerIds
                ,CAST(NULL AS String) AS caseManagerIds
                ,CAST(NULL AS String) AS leadInspectorIds
                ,CAST(NULL AS String) AS environmentalServicesOfficerIds
                ,CAST(NULL AS String) AS legalOfficerIds
                ,FALSE AS migrated
                ,'Horizon' AS ODTSourceSystem
                ,CAST(NULL AS String) AS SourceSystemID
                ,MIN(ingested_datetime) AS IngestionDate
                ,CAST(NULL AS String) AS ValidTo
                ,'' AS RowID
                ,CAST(NULL AS BOOLEAN) As isMaterialChange
                ,'Y' AS IsActive
            GROUP BY
                CAST(Horizon.caseNodeId AS Long)
                ,Horizon.casereference
                ,Horizon.projectname
                ,Horizon.projectNameWelsh
                ,Horizon.projectDescription
                ,Horizon.projectDescriptionWelsh
                ,Horizon.summary
                ,Horizon.caseCreatedDate
                ,CASE WHEN LOWER(projectstatus) = 'not published' THEN 'unpublished' ELSE LOWER(projectstatus) END
                ,sector
                ,projecttype
                ,stage
                ,projectLocation
                ,Horizon.projectLocationWelsh
                ,projectEmailAddress
                ,LOWER(Region)
                ,CAST(transboundary AS Boolean) 
                ,CAST(easting AS Integer)
                ,CAST(northing AS Integer)
                ,CAST(welshLanguage AS Boolean)
                ,LOWER(mapZoomLevel)
                ,sos
                ,datePINSFirstNotifiedOfProject
                ,dateProjectAppearsOnWebsite
                ,anticipatedSubmissionDateNonSpecific
                ,anticipatedDateOfSubmission
                ,screeningOpinionSought
                ,screeningOpinionIssued
                ,scopingOpinionSought
                ,scopingOpinionIssued
                ,section46Notification
                ,dateOfDCOSubmission
                ,deadlineForAcceptanceDecision
                ,dateOfDCOAcceptance
                ,dateOfRepresentationPeriodOpen
                ,dateOfRelevantRepresentationClose
                ,extensionToDateRelevantRepresentationsClose
                ,dateRRepAppearOnWebsite
                ,preliminaryMeetingStartDate
                ,dateSection58NoticeReceived
                ,confirmedStartOfExamination
                ,deadlineForCloseOfExamination
                ,dateTimeExaminationEnds
                ,stage4ExtensionToExamCloseDate
                ,deadlineForSubmissionOfRecommendation
                ,dateOfRecommendations
                ,stage5ExtensionToRecommendationDeadline
                ,deadlineForDecision
                ,confirmedDateOfDecision
                ,stage5ExtensionToDecisionDeadline
                ,jRPeriodEndDate
                ,dateProjectWithdrawn
                ,cast(isMaterialChange AS BOOLEAN)
                --Start Horizon only fields
                ,examTimetablePublishStatus
                ,twitteraccountname
                ,exasize
                ,tene
                ,promotername
                ,applicantfirstname
                ,applicantlastname
                ,addressLine1
                ,addressLine2
                ,addressTown
                ,addressCounty
                ,Postcode
                ,applicantemailaddress
                ,applicantwebaddress
                ,applicantphonenumber
                ,applicantdescriptionofproject
                ,HorizonCaseNumber
                """)

    def load_data(self, **kwargs):
        service_bus_data = self._load_service_bus_data()
        first_seen_service_bus_data = self._load_first_seen_service_bus_record()
        horizon_data = self._load_horizon_data()
        return {"service_bus_data": service_bus_data, "horizon_data": horizon_data, "first_seen_service_bus_data": first_seen_service_bus_data}
    
    def _parse_col_into_json(self, data: DataFrame, col_name: str, schema: T.DataType):
        """
        Convert a string column into JSON using the provided schema
        """
        return data.withColumn(col_name, F.from_json(F.regexp_replace(F.col("invoices").cast("string"), "'", '"'), schema))
    
    def _clean_json_cols(self, data: DataFrame):
        """
        Convert string columns in the horizon data into JSON
        """
        invoices_schema = T.ArrayType(
            T.StructType(
                [
                    T.StructField("invoiceStage", T.StringType(), True),
                    T.StructField("invoiceNumber", T.StringType(), True),
                    T.StructField("amountDue", T.DoubleType(), True),
                    T.StructField("paymentDueDate", T.StringType(), True),
                    T.StructField("invoicedDate", T.StringType(), True),
                    T.StructField("paymentDate", T.StringType(), True),
                    T.StructField("refundCreditNoteNumber", T.StringType(), True),
                    T.StructField("refundAmount", T.DoubleType(), True),
                    T.StructField("refundIssueDate", T.StringType(), True),
                ]
            )
        )
        meetings_schema = T.ArrayType(
            T.StructType(
                [
                    T.StructField("meetingId", T.StringType(), True),
                    T.StructField("meetingAgenda", T.StringType(), True),
                    T.StructField("planningInspectorateRole", T.StringType(), True),
                    T.StructField("meetingDate", T.StringType(), True),
                    T.StructField("meetingType", T.StringType(), True),
                    T.StructField("estimatedPrelimMeetingDate", T.StringType(), True),
                ]
            )
        )
        return self._parse_col_into_json(
            self._parse_col_into_json(data, "invoices", invoices_schema),
            "meetings",
            meetings_schema
        )

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        first_seen_service_bus_data: DataFrame = self.load_parameter("first_seen_service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)
        # Join horizon and service bus data
        horizon_data = horizon_data.join(
            service_bus_data,
            on=(
                first_seen_service_bus_data["caseId"]
                == horizon_data["HorizonCaseNumber"] & first_seen_service_bus_data["ingested"]
                == horizon_data["ingested_datetime"]
            ),
            how="left",
        ).filter(F.col("caseId").isNull())

        # Build Ids
        horizon_data_grouped = horizon_data.groupBy("caseid", "IngestionDate")
        grouping_cols = (
            "nsipOfficerIds",
            "nsipAdministrationOfficerIds",
            "inspectorIds",
            "operationsManagerIds",
            "legalOfficerIds",
            "operationsLeadIds",
            "caseManagerIds",
            "leadInspectorIds",
            "environmentalServicesOfficerIds",
        )
        grouping_cols_map = {"region": "regions"} | {x: x for x in grouping_cols}
        for col, alias in grouping_cols_map.items():
            sub_grouping = horizon_data_grouped.agg(F.collect_list(col).alias(alias))
            horizon_data = horizon_data.drop(col).join(sub_grouping, on=["caseid", "IngestionDate"], how="inner")

        # Sort columns into same order as service bus
        horizon_data = horizon_data.select(service_bus_data.columns)

        # Clean the columns that contain json strings
        horizon_data = self._clean_json_cols(horizon_data)

        results = service_bus_data.unionByName(horizon_data, allowMissingColumns=True)

        end_exec_time = datetime.now()

        data_to_write = {
            self.harmonised_table: {
                "data": results,
                "storage_kind": "ADLSG2-LegacyDelta",
                "database_name": "odw_harmonised_db",
                "table_name": self.entity,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "partition_cols": ["IsActive"],
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.harmonised_table,
                insert_count=horizon_data.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
