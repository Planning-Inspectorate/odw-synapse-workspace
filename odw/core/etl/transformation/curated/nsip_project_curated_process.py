from typing import Any
from odw.core.etl.transformation.curated.curation_process import CurationProcess
from odw.core.etl.etl_result import ETLSuccessResult, ETLResult
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, DateType
from datetime import datetime
from typing import Dict


class NsipProjectCuratedProcess(CurationProcess):
    # Based on the nsip_data notebook
    HARMONISED_TABLE = "nsip_project"
    OUTPUT_TABLE = "nsip_project"

    def get_name(self) -> str:
        return "nsip_project_curated_process"

    def _load_nsip_data(self):
        LoggingUtil().log_info("Loading data from curated NSIP prject table")
        return self.spark.sql(f"""
            SELECT
                Project.caseId
                ,Project.caseReference
                ,Project.projectName
                ,Project.projectNameWelsh
                ,Project.projectDescription
                ,Project.projectDescriptionWelsh
                ,Project.decision
                ,project.publishStatus
                ,Project.sector
                ,Project.ProjectType
                ,Project.ODTSourceSystem 
                ,Project.Stage
                ,Project.projectLocation
                ,Project.projectLocationWelsh
                ,Project.projectEmailAddress
                ,Project.regions
                ,Project.transboundary
                ,project.Easting
                ,project.Northing
                ,Project.WelshLanguage
                ,Project.mapZoomLevel
                ,Project.SecretaryOfState
                -- Pre-application dates
                ,Project.datePINSFirstNotifiedOfProject
                ,Project.dateProjectAppearsOnWebsite	
                ,Project.anticipatedSubmissionDateNonSpecific
                ,Project.anticipatedDateOfSubmission
                ,Project.screeningOpinionSought	
                ,Project.screeningOpinionIssued
                ,Project.scopingOpinionSought
                ,Project.scopingOpinionIssued
                ,Project.section46Notification
                -- acceptance dates
                ,Project.dateOfDCOSubmission
                ,Project.deadlineForAcceptanceDecision
                ,Project.dateOfDCOAcceptance
                ,Project.dateOfNonAcceptance
                --pre examination dates
                ,Project.dateOfRepresentationPeriodOpen
                ,Project.dateOfRelevantRepresentationClose
                ,Project.extensionToDateRelevantRepresentationsClose
                ,Project.dateRRepAppearOnWebsite
                ,Project.dateIAPIDue
                ,Project.rule6LetterPublishDate
                ,Project.preliminaryMeetingStartDate
                ,Project.notificationDateForPMAndEventsDirectlyFollowingPM
                ,Project.notificationDateForEventsDeveloper
                -- examination dates
                ,Project.dateSection58NoticeReceived
                ,Project.confirmedStartOfExamination
                ,Project.rule8LetterPublishDate
                ,Project.deadlineForCloseOfExamination
                ,Project.dateTimeExaminationEnds
                ,Project.stage4ExtensionToExamCloseDate
                --recommendation dates
                ,Project.deadlineForSubmissionOfRecommendation
                ,Project.dateOfRecommendations
                ,Project.stage5ExtensionToRecommendationDeadline
                --decision dates
                ,Project.deadlineForDecision
                ,Project.confirmedDateOfDecision
                ,Project.stage5ExtensionToDecisionDeadline
                --post decision dates
                ,Project.jRPeriodEndDate
                --withdrawl dates
                ,Project.dateProjectWithdrawn
                -- Additional fields
                ,Project.operationsLeadId
                ,Project.operationsManagerId
                ,Project.caseManagerId
                ,Project.nsipOfficerIds
                ,Project.nsipAdministrationOfficerIds
                ,Project.leadInspectorId
                ,Project.inspectorIds
                ,Project.environmentalServicesOfficerId
                ,Project.legalOfficerId
                ,Project.applicantId
                ,Project.migrationStatus
                ,Project.dateOfReOpenRelevantRepresentationStart
                ,Project.dateOfReOpenRelevantRepresentationClose
                ,Project.isMaterialChange
                -- Additional columns-2324
                ,Project.inceptionMeetingDate
                ,Project.draftDocumentSubmissionDate
                ,Project.programmeDocumentSubmissionDate
                ,Project.estimatedScopingSubmissionDate
                ,Project.consultationMilestoneAdequacyDate
                ,Project.principalAreaDisagreementSummaryStmtSubmittedDate
                ,Project.policyComplianceDocumentSubmittedDate
                ,Project.designApproachDocumentSubmittedDate
                ,Project.caAndTpEvidenceSubmittedDate
                ,Project.caseTeamIssuedCommentsDate
                ,Project.fastTrackAdmissionDocumentSubmittedDate
                ,Project.matureOutlineControlDocumentSubmittedDate
                ,Project.memLastUpdated
                ,Project.multipartyApplicationCheckDocumentSubmittedDate
                ,Project.programmeDocumentReviewedByEstDate
                ,Project.publicSectorEqualityDutySubmittedDate
                ,Project.statutoryConsultationPeriodEndDate
                ,Project.submissionOfDraftDocumentsDate
                ,Project.updatedProgrammeDocumentReceivedDate
                ,Project.courtDecisionDate
                ,Project.decisionChallengeSubmissionDate
                ,Project.courtDecisionOutcomeText
                ,Project.recommendation
                ,Project.additionalComments
                ,Project.caAndTpEvidence
                ,Project.fastTrackAdmissionDocument
                ,Project.meetings
                ,Project.multipartyApplicationCheckDocument
                ,Project.newMaturity
                ,Project.numberBand2Inspectors
                ,Project.numberBand3Inspectors
                ,Project.programmeDocumentURI
                ,Project.publicSectorEqualityDuty
                ,Project.subProjectType
                ,Project.tier
                ,Project.s61SummaryURI
                ,Project.planProcessEvidence
                ,Project.issuesTracker
                ,Project.essentialFastTrackComponents
                ,Project.principalAreaDisagreementSummaryStmt
                ,Project.policyComplianceDocument
                ,Project.designApproachDocument
                ,Project.matureOutlineControlDocument
                ,Project.invoices
                ,Project.operationsLeadIds
                ,Project.operationsManagerIds
                ,Project.caseManagerIds
                ,Project.leadInspectorIds
                ,Project.environmentalServicesOfficerIds
                ,Project.legalOfficerIds
            FROM 
                odw_harmonised_db.{self.HARMONISED_TABLE} AS Project
            WHERE 
                Project.IsActive='Y'
            """)

    def load_data(self, **kwargs) -> dict[str, Any]:
        nsip_project_data = self._load_nsip_data()
        return {"harmonised_data": nsip_project_data}

    def process(self, **kwargs):
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        nsip_project_data: DataFrame = self.load_parameter("harmonised_data", source_data)
        nsip_project_data = (
            nsip_project_data.withColumn("caseId", F.col("caseId").cast(IntegerType()))
            .withColumn("publishStatus", F.lower(F.col("publishStatus")))
            .withColumn("projectType", F.initcap(F.col("ProjectType")))
            .withColumn("stage", F.lower(F.regexp_replace(F.col("Stage"), r"[- ]", "_")))
            .withColumn(
                "sourceSystem",
                F.lower(F.when(F.col("ODTSourceSystem") == "ODT", F.lit("back-office-applications")).otherwise(F.col("ODTSourceSystem"))),
            )
            .drop("ODTSourceSystem")
            .withColumn("easting", F.col("easting").cast(IntegerType()))
            .withColumn("northing", F.col("northing").cast(IntegerType()))
            .withColumnRenamed("WelshLanguage", "welshLanguage")
            .withColumnRenamed("SecretaryOfState", "secretaryOfState")
            .withColumn("rule8LetterPublishDate", F.col("rule8LetterPublishDate").cast(DateType()))
        )
        end_exec_time = datetime.now()

        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": nsip_project_data,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_curated_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-curated",
                "blob_path": self.OUTPUT_TABLE,
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
                insert_count=nsip_project_data.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
