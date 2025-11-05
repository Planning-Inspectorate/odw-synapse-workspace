import json
from typing import Dict, Any, List
from abc import ABC, abstractmethod
from dotenv import load_dotenv
import os


"""
Script to perform preprocessing on specific workspace artifacts.

# Usage
`python3 pipelines/scripts/preprocess_artifacts.py`

"""
load_dotenv(verbose=True, override=True)


class ArtifactPreprocessor():
    """
    Abstract class to suppor the preprocessing of workspace artifact json
    """
    @classmethod
    def _read_artifact(cls, artifact_path: str) -> Dict[str, Any]:
        with open(artifact_path, "r") as f:
            return json.load(f)

    @classmethod
    def _write_artifact(self, artifact_path: str, artifact_json: Dict[str, Any]):
        with open(artifact_path, "w") as f:
            json.dump(artifact_json, f, indent="\t", ensure_ascii=False)

    @classmethod
    @abstractmethod
    def preprocess(cls):
       """
       Override this function to read/modify/write a json artifact
       """
       pass


class OpenLineageSparkConfigPreProcessor(ArtifactPreprocessor):
    @classmethod
    def preprocess(cls):
        artifact_path = "workspace/sparkConfiguration/OpenLineageSparkConfig.json"
        env = os.environ.get("ENV")
        function_key = os.environ.get("OL_RECEIVER_FUNCTION_KEY", "")
        artifact_json = cls._read_artifact(artifact_path)
        artifact_json["properties"]["configs"] |= {
            "spark.openlineage.transport.url": function_key,
            "spark.openlineage.namespace": f"pins-synw-odw-{env.lower()}-uks"
        }
        cls._write_artifact(artifact_path, artifact_json)


PRE_PROCESSORS: List[ArtifactPreprocessor] = [
    OpenLineageSparkConfigPreProcessor
]


if __name__ == "__main__":
    for preprocessor in PRE_PROCESSORS:
        preprocessor.preprocess()
