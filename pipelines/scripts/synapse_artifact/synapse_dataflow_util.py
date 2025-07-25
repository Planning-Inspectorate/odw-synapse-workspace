from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any


class SynapseDataFlowUtil(SynapseArtifactUtil):
    """
        Class for managing the retrieval and analysis of Synapse Data Flow artifacts
    """

    @classmethod
    def get_type_name(cls) -> str:
        return "dataflow"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        return self._web_request(
            f"{self.synapse_endpoint}/dataflows/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        response = self._web_request(
            f"{self.synapse_endpoint}/dataflows?api-version=2020-12-01",
        ).json()
        all_dataflows = response["value"]
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link,).json()
            all_dataflows.extend(response["value"])
        return all_dataflows

    def get_uncomparable_attributes(self) -> List[str]:
        return [
            r"^id$",
            r"^type$",
            r"^etag$",
            r"^properties\.entityState$",
            r"^properties\.created$",
            r"^properties\.lastModified$",
            r"^properties\.createdBy$",
            r"^properties\.lastModifiedBy$",
        ]

    def get_nullable_attributes(self) -> List[str]:
        return [
            r"^properties\.folder$",
            r"^properties\.description$",
        ]

    def get_env_attributes_to_replace(self) -> List[str]:
        return []
