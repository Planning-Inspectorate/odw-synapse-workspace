from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window
from datetime import datetime
from typing import Dict


class NsipProjectHarmonisationProcess(HarmonisationProcess):
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
    HORIZON_TABLE = "horizon_nsip_data"
    OUTPUT_TABLE = "nsip_project"

    def __init__(self, spark, debug: bool = False):
        super().__init__(spark, debug)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"
        self.service_bus_table_path = f"{self.hrm_db}.{self.HARMONISED_TABLE}"
        self.horizon_table_path = f"{self.std_db}.{self.HORIZON_TABLE}"
        self.harmonised_table_path = f"{self.hrm_db}.{self.OUTPUT_TABLE}"

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

            FROM {self.service_bus_table_path}
            """)

    def _load_first_seen_service_bus_record(self):
        return self.spark.sql(
            f"""
            (   SELECT
                    caseId
                    ,MIN(IngestionDate) AS ingested
                FROM
                    {self.service_bus_table_path}
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
            FROM {self.horizon_table_path} AS Horizon
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
        return data.withColumn(col_name, F.from_json(F.regexp_replace(F.col(col_name).cast("string"), "'", '"'), schema))

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
                on=(
                    (F.col("current.caseid") == F.col("next.caseid"))
                    & (F.col("current.ReverseOrderProcessed") - 1 == F.col("next.ReverseOrderProcessed"))
                ),
                how="left",
            )
            .join(service_bus_data.select(F.col("caseid")).distinct().alias("raw"), on=F.col("current.caseid") == F.col("raw.caseid"), how="left")
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
                F.col("caseid").cast(T.IntegerType()).alias("caseid"),
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
                F.col("transboundary").cast(T.BooleanType()).alias("transboundary"),
                F.col("easting").cast(T.IntegerType()).alias("easting"),
                F.col("northing").cast(T.IntegerType()).alias("northing"),
                F.col("welshLanguage").cast(T.BooleanType()).alias("welshLanguage"),
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
                F.lit(None).cast(T.DateType()).alias("rule8LetterPublishDate"),
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
                        F.ifnull(F.col("caseid").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caseReference").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectName").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectNameWelsh").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectDescription").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("summary").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectDescriptionWelsh").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caseCreatedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("decision").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("publishStatus").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("sector").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectType").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("sourceSystem").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("stage").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectLocation").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectLocationWelsh").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("projectEmailAddress").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("regions").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("transboundary").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("easting").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("northing").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("welshLanguage").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("mapZoomLevel").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("secretaryOfState").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("datePINSFirstNotifiedOfProject").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateProjectAppearsOnWebsite").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("anticipatedSubmissionDateNonSpecific").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("anticipatedDateOfSubmission").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("screeningOpinionSought").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("screeningOpinionIssued").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("scopingOpinionSought").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("scopingOpinionIssued").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("section46Notification").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfDCOSubmission").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("deadlineForAcceptanceDecision").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfDCOAcceptance").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfNonAcceptance").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfRepresentationPeriodOpen").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfRelevantRepresentationClose").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("extensionToDateRelevantRepresentationsClose").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateRRepAppearOnWebsite").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateIAPIDue").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("rule6LetterPublishDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("preliminaryMeetingStartDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("notificationDateForPMAndEventsDirectlyFollowingPM").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("notificationDateForEventsDeveloper").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateSection58NoticeReceived").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("confirmedStartOfExamination").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("rule8LetterPublishDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("deadlineForCloseOfExamination").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateTimeExaminationEnds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("stage4ExtensionToExamCloseDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("deadlineForSubmissionOfRecommendation").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfRecommendations").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("stage5ExtensionToRecommendationDeadline").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("deadlineForDecision").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("confirmedDateOfDecision").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("stage5ExtensionToDecisionDeadline").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("jRPeriodEndDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateProjectWithdrawn").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("operationsLeadIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("operationsManagerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caseManagerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("nsipOfficerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("nsipAdministrationOfficerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("leadInspectorId").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("inspectorIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("environmentalServicesOfficerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("legalOfficerId").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantId").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("migrationStatus").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfReOpenRelevantRepresentationStart").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("dateOfReOpenRelevantRepresentationClose").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("migrated").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("ODTSourceSystem").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("SourceSystemID").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("IngestionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("ValidTo").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("IsActive").cast(T.StringType()), F.lit(".")),
                        # Start Horizon only fields
                        F.ifnull(F.col("examTimetablePublishStatus").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("twitteraccountname").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("exasize").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("tene").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("promotername").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantfirstname").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantlastname").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("addressLine1").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("addressLine2").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("addressTown").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("addressCounty").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("Postcode").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantemailaddress").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantwebaddress").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantphonenumber").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("applicantdescriptionofproject").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("HorizonCaseNumber").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("isMaterialChange").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("inceptionMeetingDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("draftDocumentSubmissionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("programmeDocumentSubmissionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("estimatedScopingSubmissionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("consultationMilestoneAdequacyDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("principalAreaDisagreementSummaryStmtSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("policyComplianceDocumentSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("designApproachDocumentSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caAndTpEvidenceSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caseTeamIssuedCommentsDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("fastTrackAdmissionDocumentSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("matureOutlineControlDocumentSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("memLastUpdated").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("multipartyApplicationCheckDocumentSubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("programmeDocumentReviewedByEstDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("publicSectorEqualityDutySubmittedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("statutoryConsultationPeriodEndDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("submissionOfDraftDocumentsDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("updatedProgrammeDocumentReceivedDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("courtDecisionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("decisionChallengeSubmissionDate").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("courtDecisionOutcomeText").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("recommendation").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("additionalComments").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caAndTpEvidence").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("fastTrackAdmissionDocument").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("meetings").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("multipartyApplicationCheckDocument").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("newMaturity").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("numberBand2Inspectors").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("numberBand3Inspectors").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("programmeDocumentURI").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("publicSectorEqualityDuty").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("subProjectType").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("tier").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("s61SummaryURI").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("planProcessEvidence").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("issuesTracker").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("essentialFastTrackComponents").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("principalAreaDisagreementSummaryStmt").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("policyComplianceDocument").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("designApproachDocument").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("matureOutlineControlDocument").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("invoices").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("operationsLeadIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("operationsManagerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("caseManagerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("leadInspectorIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("environmentalServicesOfficerIds").cast(T.StringType()), F.lit(".")),
                        F.ifnull(F.col("legalOfficerIds").cast(T.StringType()), F.lit(".")),
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
        horizon_data = (
            horizon_data.join(
                first_seen_service_bus_data,
                on=(
                    (horizon_data["HorizonCaseNumber"] == first_seen_service_bus_data["caseId"])
                    & (horizon_data["IngestionDate"] >= first_seen_service_bus_data["ingested"])
                ),
                how="left",
            )
            .filter(first_seen_service_bus_data["caseId"].isNull())
            .drop(first_seen_service_bus_data["caseid"])
        )

        # Build Ids
        horizon_data_grouped = horizon_data.groupBy(horizon_data["caseid"], "IngestionDate")
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
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-LegacyDelta",
                "database_name": "odw_harmonised_db",
                "table_name": self.OUTPUT_TABLE,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "nsip_project",
                "partition_cols": ["IsActive"],
                "write_options": {"overwriteSchema": "true"},
                "write_mode": "overwrite",
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.harmonised_table_path,
                insert_count=final_df.count(),
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
