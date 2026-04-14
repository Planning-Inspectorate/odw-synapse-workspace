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
from pyspark.sql import Window
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
    HARMONISED_TABLE = "sb_nsip_project"
    OUTPUT_TABLE = "nsip_project"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"
        self.service_bus_table = f"{self.hrm_db}.{self.HARMONISED_TABLE}"
        self.horizon_table = f"{self.std_db}.horizon_nsip_data"
        self.harmonised_table = f"{self.hrm_db}.{self.OUTPUT_TABLE}"

    @classmethod
    def get_name(cls):
        return "NSIP Project Harmonisation"

    def _load_service_bus_data(self):
        return self.spark.sql(f"""
            SELECT 
                NSIPProjectInfoInternalID,
                caseId,
                caseReference,
                projectName,
                projectNameWelsh,
                projectDescription,
                projectDescriptionWelsh,
                CAST(NULL AS string) AS summary,
                CAST(NULL AS string) AS caseCreatedDate,
                decision,
                publishStatus,
                sector,
                projectType,
                sourceSystem,
                stage,
                projectLocation,
                projectLocationWelsh,
                projectEmailAddress,
                regions,
                CAST(transboundary AS Boolean) AS transboundary,
                CAST(easting AS Integer) AS easting,
                CAST(northing AS Integer) AS northing,
                welshLanguage,
                mapZoomLevel,
                secretaryOfState,
                datePINSFirstNotifiedOfProject,
                dateProjectAppearsOnWebsite,
                anticipatedSubmissionDateNonSpecific,
                anticipatedDateOfSubmission,
                screeningOpinionSought,
                screeningOpinionIssued,
                scopingOpinionSought,
                scopingOpinionIssued,
                section46Notification,
                dateOfDCOSubmission,
                deadlineForAcceptanceDecision,
                dateOfDCOAcceptance,
                dateOfNonAcceptance,
                dateOfRepresentationPeriodOpen,
                dateOfRelevantRepresentationClose,
                extensionToDateRelevantRepresentationsClose,
                dateRRepAppearOnWebsite,
                dateIAPIDue,
                rule6LetterPublishDate,
                preliminaryMeetingStartDate,
                notificationDateForPMAndEventsDirectlyFollowingPM,
                notificationDateForEventsDeveloper,
                dateSection58NoticeReceived,
                confirmedStartOfExamination,
                rule8LetterPublishDate,
                deadlineForCloseOfExamination,
                dateTimeExaminationEnds,
                stage4ExtensionToExamCloseDate,
                deadlineForSubmissionOfRecommendation,
                dateOfRecommendations,
                stage5ExtensionToRecommendationDeadline,
                deadlineForDecision,
                confirmedDateOfDecision,
                stage5ExtensionToDecisionDeadline,
                jRPeriodEndDate,
                dateProjectWithdrawn,
                operationsLeadId,
                operationsManagerId,
                caseManagerId,
                nsipOfficerIds,
                nsipAdministrationOfficerIds,
                leadInspectorId,
                inspectorIds,
                environmentalServicesOfficerId,
                legalOfficerId,
                applicantId,
                CAST(migrationStatus AS Boolean) AS migrationStatus,
                dateOfReOpenRelevantRepresentationStart,
                dateOfReOpenRelevantRepresentationClose,
                IsActive,

                -- Horizon only fields
                CAST(NULL AS String) AS examTimetablePublishStatus,
                CAST(NULL AS String) AS twitteraccountname,
                CAST(NULL AS String) AS exasize,
                CAST(NULL AS String) AS tene,
                CAST(NULL AS String) AS promotername,
                CAST(NULL AS String) AS applicantfirstname,
                CAST(NULL AS String) AS applicantlastname,
                CAST(NULL AS String) AS addressLine1,
                CAST(NULL AS String) AS addressLine2,
                CAST(NULL AS String) AS addressTown,
                CAST(NULL AS String) AS addressCounty,
                CAST(NULL AS String) AS Postcode,
                CAST(NULL AS String) AS applicantemailaddress,
                CAST(NULL AS String) AS applicantwebaddress,
                CAST(NULL AS String) AS applicantphonenumber,
                CAST(NULL AS String) AS applicantdescriptionofproject,
                CAST(NULL AS String) AS HorizonCaseNumber,

                -- Additional columns-2323
                inceptionMeetingDate,
                draftDocumentSubmissionDate,
                programmeDocumentSubmissionDate,
                estimatedScopingSubmissionDate,
                consultationMilestoneAdequacyDate,
                principalAreaDisagreementSummaryStmtSubmittedDate,
                policyComplianceDocumentSubmittedDate,
                designApproachDocumentSubmittedDate,
                caAndTpEvidenceSubmittedDate,
                caseTeamIssuedCommentsDate,
                fastTrackAdmissionDocumentSubmittedDate,
                matureOutlineControlDocumentSubmittedDate,
                memLastUpdated,
                multipartyApplicationCheckDocumentSubmittedDate,
                programmeDocumentReviewedByEstDate,
                publicSectorEqualityDutySubmittedDate,
                statutoryConsultationPeriodEndDate,
                submissionOfDraftDocumentsDate,
                updatedProgrammeDocumentReceivedDate,
                courtDecisionDate,
                decisionChallengeSubmissionDate,
                courtDecisionOutcomeText,
                recommendation,
                additionalComments,
                caAndTpEvidence,
                fastTrackAdmissionDocument,
                meetings,
                multipartyApplicationCheckDocument,
                newMaturity,
                numberBand2Inspectors,
                numberBand3Inspectors,
                programmeDocumentURI,
                publicSectorEqualityDuty,
                subProjectType,
                tier,
                s61SummaryURI,
                planProcessEvidence,
                issuesTracker,
                essentialFastTrackComponents,
                principalAreaDisagreementSummaryStmt,
                policyComplianceDocument,
                designApproachDocument,
                matureOutlineControlDocument,
                invoices,
                operationsLeadIds,
                operationsManagerIds,
                caseManagerIds,
                leadInspectorIds,
                environmentalServicesOfficerIds,
                legalOfficerIds,

                -- Metadata
                TRUE AS migrated,
                ODTSourceSystem,
                SourceSystemID,
                CAST(IngestionDate AS timestamp) IngestionDate ,
                ValidTo,
                '' AS RowID,
                isMaterialChange

            FROM {self.service_bus_table}
            """)

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
        return self._parse_col_into_json(self._parse_col_into_json(data, "invoices", invoices_schema), "meetings", meetings_schema)

    def _clean_data(self, service_bus_data: DataFrame, combined_data: DataFrame):
        """
        Need to work out what this is actually doing
        """
        # Need to read the original table
        w_reverse_per_case = Window.partitionBy("caseid").orderBy(F.col("IngestionDate").desc())
        w_internal_id = Window.orderBy(F.col("IngestionDate").asc(), F.col("caseid").asc())

        out_df = combined_data.select(
            F.row_number().over(w_reverse_per_case).alias("ReverseOrderProcessed"),
            F.row_number().over(w_internal_id).alias("NSIPProjectInfoInternalID"),
            F.col("caseid"),
            F.col("IngestionDate"),
            F.col("ValidTo"),
            F.lit("0").alias("migrated"),
            F.when(F.row_number().over(w_reverse_per_case) == 1, F.lit("Y")).otherwise(F.lit("N")).alias("IsActive"),
        )

        df_calcs = (
            out_df.alias("current")
            .join(
                out_df.alias("next"),
                on=(F.col("current.caseid") == F.col("next.caseid") & F.col("current.ReverseOrderProcessed") == F.col("next.ReverseOrderProcessed")),
                how="left",
            )
            .join(service_bus_data.select(F.col("caseid")).alias("raw"), on=F.col("current.caseid") == F.col("raw.caseid"), how="left")
            .select(
                F.col("current.NSIPProjectInfoInternalID"),
                F.col("current.caseid").alias("temp_caseid"),
                F.col("current.IngestionDate").alias("temp_IngestionDate"),
                F.coalesce(F.col("current.ValidTo"), F.col("next.IngestionDate")).alias("ValidTo"),
                F.when(F.col("raw.caseid").isNotNull(), F.lit("1")).otherwise(F.lit("0")).alias("migrated"),
                F.col("current.IsActive"),
            )
        )

        cleaned_data = combined_data.select(
            [
                F.col("NSIPProjectInfoInternalID"),
                F.col("caseid").cast(T.IntegerType).alias("caseid"),
                F.col("casereference"),
                F.col("projectname"),
                F.col("projectNameWelsh"),
                F.col("projectDescription"),
                F.col("summary"),
                F.col("projectDescriptionWelsh"),
                F.col("caseCreatedDate"),
                F.col("decision"),
                F.col("publishStatus"),
                F.col("sector"),
                F.col("projectType"),
                F.col("sourceSystem"),
                F.col("stage"),
                F.col("projectLocation"),
                F.col("projectLocationWelsh"),
                F.col("projectEmailAddress"),
                F.col("Regions"),
                F.col("transboundary").cast(T.BooleanType).alias("transboundary"),
                F.col("easting").cast(T.IntegerType).alias("easting"),
                F.col("northing").cast(T.IntegerType).alias("northing"),
                F.col("welshLanguage").cast(T.BooleanType).alias("welshLanguage"),
                F.col("mapZoomLevel"),
                F.col("secretaryOfState"),
                F.col("datePINSFirstNotifiedOfProject"),
                F.col("dateProjectAppearsOnWebsite"),
                F.col("anticipatedSubmissionDateNonSpecific"),
                F.col("anticipatedDateOfSubmission"),
                F.col("screeningOpinionSought"),
                F.col("screeningOpinionIssued"),
                F.col("scopingOpinionSought"),
                F.col("scopingOpinionIssued"),
                F.col("section46Notification"),
                F.col("dateOfDCOSubmission"),
                F.col("deadlineForAcceptanceDecision"),
                F.col("dateOfDCOAcceptance"),
                F.col("dateOfNonAcceptance"),
                F.col("dateOfRepresentationPeriodOpen"),
                F.col("dateOfRelevantRepresentationClose"),
                F.col("extensionToDateRelevantRepresentationsClose"),
                F.col("dateRRepAppearOnWebsite"),
                F.col("dateIAPIDue"),
                F.col("rule6LetterPublishDate"),
                F.col("preliminaryMeetingStartDate"),
                F.col("notificationDateForPMAndEventsDirectlyFollowingPM"),
                F.col("notificationDateForEventsDeveloper"),
                F.col("dateSection58NoticeReceived"),
                F.col("confirmedStartOfExamination"),
                F.lit(None).cast(T.DateType).alias("rule8LetterPublishDate"),
                F.col("deadlineForCloseOfExamination"),
                F.col("dateTimeExaminationEnds"),
                F.col("stage4ExtensionToExamCloseDate"),
                F.col("deadlineForSubmissionOfRecommendation"),
                F.col("dateOfRecommendations"),
                F.col("stage5ExtensionToRecommendationDeadline"),
                F.col("deadlineForDecision"),
                F.col("confirmedDateOfDecision"),
                F.col("stage5ExtensionToDecisionDeadline"),
                F.col("jRPeriodEndDate"),
                F.col("dateProjectWithdrawn"),
                F.col("operationsLeadId"),
                F.col("operationsManagerId"),
                F.col("caseManagerId"),
                F.col("nsipOfficerIds"),
                F.col("nsipAdministrationOfficerIds"),
                F.col("leadInspectorId"),
                F.col("inspectorIds"),
                F.col("environmentalServicesOfficerId"),
                F.col("legalOfficerId"),
                F.col("applicantId"),
                F.col("migrationStatus"),
                F.col("dateOfReOpenRelevantRepresentationStart"),
                F.col("dateOfReOpenRelevantRepresentationClose"),
                # Start Horizon only fields
                F.col("examTimetablePublishStatus"),
                F.col("twitteraccountname"),
                F.col("exasize"),
                F.col("tene"),
                F.col("promotername"),
                F.col("applicantfirstname"),
                F.col("applicantlastname"),
                F.col("addressLine1"),
                F.col("addressLine2"),
                F.col("addressTown"),
                F.col("addressCounty"),
                F.col("Postcode"),
                F.col("applicantemailaddress"),
                F.col("applicantwebaddress"),
                F.col("applicantphonenumber"),
                F.col("applicantdescriptionofproject"),
                F.col("HorizonCaseNumber"),
                # Additional columns-2323
                F.col("inceptionMeetingDate"),
                F.col("draftDocumentSubmissionDate"),
                F.col("programmeDocumentSubmissionDate"),
                F.col("estimatedScopingSubmissionDate"),
                F.col("consultationMilestoneAdequacyDate"),
                F.col("principalAreaDisagreementSummaryStmtSubmittedDate"),
                F.col("policyComplianceDocumentSubmittedDate"),
                F.col("designApproachDocumentSubmittedDate"),
                F.col("caAndTpEvidenceSubmittedDate"),
                F.col("caseTeamIssuedCommentsDate"),
                F.col("fastTrackAdmissionDocumentSubmittedDate"),
                F.col("matureOutlineControlDocumentSubmittedDate"),
                F.col("memLastUpdated"),
                F.col("multipartyApplicationCheckDocumentSubmittedDate"),
                F.col("programmeDocumentReviewedByEstDate"),
                F.col("publicSectorEqualityDutySubmittedDate"),
                F.col("statutoryConsultationPeriodEndDate"),
                F.col("submissionOfDraftDocumentsDate"),
                F.col("updatedProgrammeDocumentReceivedDate"),
                F.col("courtDecisionDate"),
                F.col("decisionChallengeSubmissionDate"),
                F.col("courtDecisionOutcomeText"),
                F.col("recommendation"),
                F.col("additionalComments"),
                F.col("caAndTpEvidence"),
                F.col("fastTrackAdmissionDocument"),
                F.col("meetings"),
                F.col("multipartyApplicationCheckDocument"),
                F.col("newMaturity"),
                F.col("numberBand2Inspectors"),
                F.col("numberBand3Inspectors"),
                F.col("programmeDocumentURI"),
                F.col("publicSectorEqualityDuty"),
                F.col("subProjectType"),
                F.col("tier"),
                F.col("s61SummaryURI"),
                F.col("planProcessEvidence"),
                F.col("issuesTracker"),
                F.col("essentialFastTrackComponents"),
                F.col("principalAreaDisagreementSummaryStmt"),
                F.col("policyComplianceDocument"),
                F.col("designApproachDocument"),
                F.col("matureOutlineControlDocument"),
                F.col("invoices"),
                F.col("operationsLeadIds"),
                F.col("operationsManagerIds"),
                F.col("caseManagerIds"),
                F.col("leadInspectorIds"),
                F.col("environmentalServicesOfficerIds"),
                F.col("legalOfficerIds"),
                # End Horizon only fields
                F.col("migrated"),
                F.col("ODTSourceSystem"),
                F.col("SourceSystemID"),
                F.col("IngestionDate"),
                F.col("ValidTo"),
                F.md5(
                    F.coalesce(
                        F.ifnull(F.cast(T.StringType, F.col("caseid")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caseReference")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectName")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectNameWelsh")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectDescription")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("summary")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectDescriptionWelsh")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caseCreatedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("decision")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("publishStatus")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("sector")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectType")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("sourceSystem")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("stage")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectLocation")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectLocationWelsh")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("projectEmailAddress")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("regions")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("transboundary")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("easting")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("northing")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("welshLanguage")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("mapZoomLevel")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("secretaryOfState")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("datePINSFirstNotifiedOfProject")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateProjectAppearsOnWebsite")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("anticipatedSubmissionDateNonSpecific")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("anticipatedDateOfSubmission")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("screeningOpinionSought")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("screeningOpinionIssued")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("scopingOpinionSought")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("scopingOpinionIssued")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("section46Notification")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfDCOSubmission")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("deadlineForAcceptanceDecision")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfDCOAcceptance")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfNonAcceptance")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfRepresentationPeriodOpen")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfRelevantRepresentationClose")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("extensionToDateRelevantRepresentationsClose")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateRRepAppearOnWebsite")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateIAPIDue")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("rule6LetterPublishDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("preliminaryMeetingStartDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("notificationDateForPMAndEventsDirectlyFollowingPM")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("notificationDateForEventsDeveloper")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateSection58NoticeReceived")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("confirmedStartOfExamination")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("rule8LetterPublishDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("deadlineForCloseOfExamination")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateTimeExaminationEnds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("stage4ExtensionToExamCloseDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("deadlineForSubmissionOfRecommendation")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfRecommendations")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("stage5ExtensionToRecommendationDeadline")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("deadlineForDecision")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("confirmedDateOfDecision")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("stage5ExtensionToDecisionDeadline")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("jRPeriodEndDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateProjectWithdrawn")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("operationsLeadIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("operationsManagerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caseManagerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("nsipOfficerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("nsipAdministrationOfficerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("leadInspectorId")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("inspectorIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("environmentalServicesOfficerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("legalOfficerId")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantId")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("migrationStatus")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfReOpenRelevantRepresentationStart")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("dateOfReOpenRelevantRepresentationClose")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("migrated")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("ODTSourceSystem")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("SourceSystemID")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("IngestionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("ValidTo")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("IsActive")), F.lit(".")),
                        # Start Horizon only fields
                        F.ifnull(F.cast(T.StringType, F.col("examTimetablePublishStatus")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("twitteraccountname")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("exasize")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("tene")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("promotername")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantfirstname")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantlastname")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("addressLine1")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("addressLine2")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("addressTown")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("addressCounty")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("Postcode")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantemailaddress")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantwebaddress")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantphonenumber")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("applicantdescriptionofproject")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("HorizonCaseNumber")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("isMaterialChange")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("inceptionMeetingDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("draftDocumentSubmissionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("programmeDocumentSubmissionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("estimatedScopingSubmissionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("consultationMilestoneAdequacyDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("principalAreaDisagreementSummaryStmtSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("policyComplianceDocumentSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("designApproachDocumentSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caAndTpEvidenceSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caseTeamIssuedCommentsDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("fastTrackAdmissionDocumentSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("matureOutlineControlDocumentSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("memLastUpdated")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("multipartyApplicationCheckDocumentSubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("programmeDocumentReviewedByEstDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("publicSectorEqualityDutySubmittedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("statutoryConsultationPeriodEndDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("submissionOfDraftDocumentsDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("updatedProgrammeDocumentReceivedDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("courtDecisionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("decisionChallengeSubmissionDate")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("courtDecisionOutcomeText")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("recommendation")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("additionalComments")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caAndTpEvidence")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("fastTrackAdmissionDocument")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("meetings")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("multipartyApplicationCheckDocument")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("newMaturity")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("numberBand2Inspectors")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("numberBand3Inspectors")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("programmeDocumentURI")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("publicSectorEqualityDuty")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("subProjectType")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("tier")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("s61SummaryURI")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("planProcessEvidence")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("issuesTracker")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("essentialFastTrackComponents")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("principalAreaDisagreementSummaryStmt")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("policyComplianceDocument")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("designApproachDocument")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("matureOutlineControlDocument")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("invoices")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("operationsLeadIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("operationsManagerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("caseManagerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("leadInspectorIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("environmentalServicesOfficerIds")), F.lit(".")),
                        F.ifnull(F.cast(T.StringType, F.col("legalOfficerIds")), F.lit(".")),
                    )
                ).alias("RowID"),
                F.col("isMaterialChange"),
                F.col("IsActive"),
            ]
        )

        columns = cleaned_data.columns
        cleaned_data = cleaned_data.drop("NSIPProjectInfoInternalID", "ValidTo", "migrated", "IsActive")
        final_df = cleaned_data.join(
            df_calcs, (df_calcs["temp_caseid"] == cleaned_data["caseid"]) & (df_calcs["temp_IngestionDate"] == cleaned_data["IngestionDate"])
        ).select(columns)
        return final_df.drop_duplicates()

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

        # After writing logic
        final_df = self._clean_data(service_bus_data, results)

        data_to_write = {
            self.harmonised_table: {
                "data": final_df,
                "storage_kind": "ADLSG2-LegacyDelta",
                "database_name": "odw_harmonised_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "nsip_project",
                "partition_cols": ["IsActive"],
                "write_options": {"overwriteSchema": "true"},
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.harmonised_table,
                insert_count=final_df.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
