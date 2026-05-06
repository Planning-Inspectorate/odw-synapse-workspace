from odw.core.etl.etl_process import ETLProcess
from odw.core.exceptions import DuplicateETLProcessNameException, ETLProcessNameNotFoundException
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess
from odw.core.etl.transformation.harmonised.nsip_document_harmonisation_process import NsipDocumentHarmonisationProcess
from odw.core.etl.transformation.harmonised.nsip_exam_timetable_harmonisation_process import NsipExamTimetableHarmonisationProcess
from odw.core.etl.transformation.harmonised.nsip_representation_harmonisation_process import NsipRepresentationHarmonisationProcess
from odw.core.etl.transformation.harmonised.nsip_s51_advice_harmonisation_process import NsipS51AdviceHarmonisationProcess
from odw.core.etl.transformation.harmonised.nsip_meeting_harmonisation_process import NsipMeetingHarmonisationProcess
from odw.core.etl.transformation.harmonised.appeal_document_harmonisation_process import AppealDocumentHarmonisationProcess
from odw.core.etl.transformation.harmonised.aie_document_harmonisation_process import AieDocumentHarmonisationProcess
from odw.core.etl.transformation.harmonised.entraid_harmonisation_process import EntraIdHarmonisationProcess
from odw.core.etl.transformation.harmonised.listed_building_harmonisation_process import ListedBuildingHarmonisationProcess
from odw.core.etl.transformation.curated.nsip_document_curated_process import NsipDocumentCuratedProcess
from odw.core.etl.transformation.curated.nsip_subscription_curated_process import NsipSubscriptionCuratedProcess
from odw.core.etl.transformation.curated.nsip_exam_timetable_curated_process import NsipExamTimetableCuratedProcess
from odw.core.etl.transformation.curated.nsip_representation_curated_process import NsipRepresentationCuratedProcess
from odw.core.etl.transformation.curated.nsip_s51_advice_curated_process import NsipS51AdviceCuratedProcess
from odw.core.etl.transformation.curated.nsip_meeting_curated_process import NsipMeetingCuratedProcess
from odw.core.etl.transformation.curated.appeal_document_curated_process import AppealDocumentCuratedProcess
from odw.core.etl.transformation.harmonised.checkmark_case_marking_harmonisation_process import CheckmarkCaseMarkingHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_comment_state_ref_harmonisation_process import CheckmarkCommentStateRefHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_comment_type_ref_harmonisation_process import CheckmarkCommentTypeRefHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_comments_harmonisation_process import CheckmarkCommentsHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_conditions_reference_harmonisation_process import CheckmarkConditionsReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_coverage_reference_harmonisation_process import CheckmarkCoverageReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_level_reference_harmonisation_process import CheckmarkLevelReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_outcome_reference_harmonisation_process import CheckmarkOutcomeReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_presentation_accuracy_detail_reference_harmonisation_process import CheckmarkPresentationAccuracyDetailReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_procedure_reference_harmonisation_process import CheckmarkProcedureReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_reading_case_harmonisation_process import CheckmarkReadingCaseHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_reading_status_reference_harmonisation_process import CheckmarkReadingStatusReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_reading_type_reference_harmonisation_process import CheckmarkReadingTypeReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_source_reference_harmonisation_process import CheckmarkSourceReferenceHarmonisationProcess
from odw.core.etl.transformation.harmonised.checkmark_structure_reasoning_detail_reference_harmonisation_process import CheckmarkStructureReasoningDetailReferenceHarmonisationProcess
from odw.core.etl.transformation.curated.checkmark_case_marking_curated_process import CheckmarkCaseMarkingCuratedProcess
from odw.core.etl.transformation.curated.checkmark_comment_state_ref_curated_process import CheckmarkCommentStateRefCuratedProcess
from odw.core.etl.transformation.curated.checkmark_comment_type_ref_curated_process import CheckmarkCommentTypeRefCuratedProcess
from odw.core.etl.transformation.curated.checkmark_comments_curated_process import CheckmarkCommentsCuratedProcess
from odw.core.etl.transformation.curated.checkmark_conditions_reference_curated_process import CheckmarkConditionsReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_coverage_reference_curated_process import CheckmarkCoverageReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_level_reference_curated_process import CheckmarkLevelReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_outcome_reference_curated_process import CheckmarkOutcomeReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_presentation_accuracy_detail_reference_curated_process import CheckmarkPresentationAccuracyDetailReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_procedure_reference_curated_process import CheckmarkProcedureReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_reading_case_curated_process import CheckmarkReadingCaseCuratedProcess
from odw.core.etl.transformation.curated.checkmark_reading_status_reference_curated_process import CheckmarkReadingStatusReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_reading_type_reference_curated_process import CheckmarkReadingTypeReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_source_reference_curated_process import CheckmarkSourceReferenceCuratedProcess
from odw.core.etl.transformation.curated.checkmark_structure_reasoning_detail_reference_curated_process import CheckmarkStructureReasoningDetailReferenceCuratedProcess







from typing import Dict, List, Set, Type
import json


class ETLProcessFactory:
    ETL_PROCESSES: Set[Type[ETLProcess]] = {
        StandardisationProcess,
        ServiceBusStandardisationProcess,
        HorizonStandardisationProcess,
        ServiceBusHarmonisationProcess,
        NsipDocumentHarmonisationProcess,
        NsipExamTimetableHarmonisationProcess,
        NsipRepresentationHarmonisationProcess,
        NsipS51AdviceHarmonisationProcess,
        NsipMeetingHarmonisationProcess,
        AppealDocumentHarmonisationProcess,
        AieDocumentHarmonisationProcess,
        EntraIdHarmonisationProcess,
        ListedBuildingHarmonisationProcess,
        NsipDocumentCuratedProcess,
        NsipSubscriptionCuratedProcess,
        NsipExamTimetableCuratedProcess,
        NsipRepresentationCuratedProcess,
        NsipS51AdviceCuratedProcess,
        NsipMeetingCuratedProcess,
        AppealDocumentCuratedProcess,
        CheckmarkCommentStateRefHarmonisationProcess,
        CheckmarkCommentTypeRefHarmonisationProcess,
        CheckmarkCommentsHarmonisationProcess,
        CheckmarkConditionsReferenceHarmonisationProcess,
        CheckmarkCoverageReferenceHarmonisationProcess,
        CheckmarkLevelReferenceHarmonisationProcess,
        CheckmarkOutcomeReferenceHarmonisationProcess,
        CheckmarkPresentationAccuracyDetailReferenceHarmonisationProcess,
        CheckmarkProcedureReferenceHarmonisationProcess,
        CheckmarkReadingCaseHarmonisationProcess,
        CheckmarkReadingStatusReferenceHarmonisationProcess,
        CheckmarkReadingTypeReferenceHarmonisationProcess,
        CheckmarkSourceReferenceHarmonisationProcess,
        CheckmarkStructureReasoningDetailReferenceHarmonisationProcess,
        CheckmarkCaseMarkingCuratedProcess,
        CheckmarkCommentStateRefCuratedProcess,
        CheckmarkCommentTypeRefCuratedProcess,
        CheckmarkCommentsCuratedProcess,
        CheckmarkConditionsReferenceCuratedProcess,
        CheckmarkCoverageReferenceCuratedProcess,
        CheckmarkLevelReferenceCuratedProcess,
        CheckmarkOutcomeReferenceCuratedProcess,
        CheckmarkPresentationAccuracyDetailReferenceCuratedProcess,
        CheckmarkProcedureReferenceCuratedProcess,
        CheckmarkReadingCaseCuratedProcess,
        CheckmarkReadingStatusReferenceCuratedProcess,
        CheckmarkReadingTypeReferenceCuratedProcess,
        CheckmarkSourceReferenceCuratedProcess,
        CheckmarkStructureReasoningDetailReferenceCuratedProcess,
        CheckmarkCaseMarkingHarmonisationProcess,
    }

    @classmethod
    def _validate_etl_process_classes(cls):
        name_map: Dict[str, List[Type[ETLProcess]]] = dict()
        for etl_process_class in cls.ETL_PROCESSES:
            type_name = etl_process_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(etl_process_class)
            else:
                name_map[type_name] = [etl_process_class]
        invalid_types = {k: v for k, v in name_map.items() if len(v) > 1}
        if invalid_types:
            raise DuplicateETLProcessNameException(
                f"The following ETLProcess implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {k: v[0] for k, v in name_map.items()}

    @classmethod
    def get(cls, etl_process_name: str) -> Type[ETLProcess]:
        etl_process_map = cls._validate_etl_process_classes()
        if etl_process_name not in etl_process_map:
            raise ETLProcessNameNotFoundException(f"No ETLProcess class could be found for ETLProcess name '{etl_process_name}'")
        return etl_process_map[etl_process_name]
