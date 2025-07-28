from pipelines.scripts.synapse_artifact.synapse_artifact_util import SynapseArtifactUtil
from typing import List, Dict, Any

class SynapseLakeDatabaseUtil(SynapseArtifactUtil):
    """
    Class for managing the retrieval and analysis of Synapse Lake Database artifacts
    """

    @classmethod
    def get_type_name(cls) -> str:
        return "database"

    def get(self, artifact_name: str, **kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Retrieves a specific Lake Database definition by name.
        """
        return self._web_request(
            f"{self.synapse_endpoint}/lakeDatabases/{artifact_name}?api-version=2020-12-01",
        ).json()

    def get_all(self, **kwargs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Retrieves all Lake Database definitions in the workspace.
        """
        response = self._web_request(
            f"{self.synapse_endpoint}/lakeDatabases?api-version=2020-12-01",
        ).json()

        all_lake_dbs = response.get("value", [])
        while "nextLink" in response:
            next_link = response["nextLink"]
            response = self._web_request(next_link).json()
            all_lake_dbs.extend(response.get("value", []))
        return all_lake_dbs

    def get_uncomparable_attributes(self) -> List[str]:
        """
        Attributes that should not be compared across environments.
        """
        return [
            r"^id$",
            r"^etag$",
            r"^type$",
            r"^properties.provisioningState$",
            r"^properties.creationDate$"
        ]

    def get_nullable_attributes(self) -> List[str]:
        """
        Attributes that might be null in some environments.
        """
        return [
            r"^properties.description$",
            r"^properties.contactDetails$"
        ]

    def get_env_attributes_to_replace(self) -> List[str]:
        """
        Attributes that may differ across environments and should be replaced dynamically.
        """
        return [
            r"^properties.defaultStorageAccountName$",
            r"^properties.defaultDataLakeStorageAccountUrl$"
        ]
