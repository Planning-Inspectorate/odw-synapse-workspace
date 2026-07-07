from typing import Any

from odw.core.etl.transformation.harmonised.harmonisation_process import (
    HarmonisationProcess,
)


class ServiceUserHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "service_user"

    SERVICE_BUS_TABLE = "sb_service_user"
    HZN_SERVICE_USER_TABLE = "horizon_case_involvement"
    HZN_NSIP_PROJECT_TABLE = "horizon_nsip_data"
    HZN_NSIP_REPRESENTATION_TABLE = "horizon_nsip_relevant_representation"

    @classmethod
    def get_name(cls) -> str:
        return "Service User Harmonisation Process"

    def load_data(self, **kwargs) -> dict[str, Any]:
        raise NotImplementedError(
            "ServiceUserHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any], **kwargs):
        raise NotImplementedError(
            "ServiceUserHarmonisationProcess.process() has not been implemented yet."
        )
