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
        Handles empty or invalid responses gracefully.
        """
        url = f"{self.synapse_endpoint}/lakeDatabases?api-version=2020-12-01"
        response = self._web_request(url)
        print(f"---- This is the endpoint----{response}")

        # If the response has no content (204) or an empty body, return empty list
        if not response.text.strip():
            return []

        try:
            data = response.json()
        except ValueError:
            raise RuntimeError(f"Invalid JSON returned from {url}: {response.text[:200]}")

        all_lake_dbs = data.get("value", [])
        while "nextLink" in data:
            next_link = data["nextLink"]
            next_response = self._web_request(next_link)

            if not next_response.text.strip():
                break

            try:
                data = next_response.json()
            except ValueError:
                raise RuntimeError(f"Invalid JSON returned from {next_link}: {next_response.text[:200]}")

            all_lake_dbs.extend(data.get("value", []))

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
