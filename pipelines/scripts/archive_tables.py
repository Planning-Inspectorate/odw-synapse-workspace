from azure.identity import AzureCliCredential, ChainedTokenCredential
import requests
import json
import os


class TableArchiveUtil():
    def __init__(self, workspace_name: str):
        self.workspace_name = workspace_name

    credential = None
    _token = None

    def get_all_unarchived_notebooks(self):
        all_notebook_files = os.listdir("workspace/notebook")
        notebook_content_map = {
            file: json.load(open(f"workspace/notebook/{file}", "r"))
            for file in all_notebook_files
        }
        unarchived = {
            file.replace(".json", ""): content
            for file, content in notebook_content_map.items()
            if "achive" not in content["properties"].get("folder", "")
        }
        return set(unarchived.keys())
    
    def get_all_unarchived_files_that_use_py_create_spark_schema(self):
        unachived_notebooks = self.get_all_unarchived_notebooks()
        return {
            file
            for file in unachived_notebooks
            if "py_create_spark_schema" in open(f"workspace/notebook/{file}.json", "r").read()
        }
    
    @classmethod
    def _get_token(cls) -> str:
        if not (cls.credential and cls._token):
            cls.credential = ChainedTokenCredential(
                # ManagedIdentityCredential(),
                AzureCliCredential()
            )
            cls._token = cls.credential.get_token("https://dev.azuresynapse.net").token
        return cls._token
    
    def _web_request(self, endpoint: str) -> requests.Response:
        """
            Submit a http request against the specified endpoint

            :param endpooint: The url to send the request to
            :return: The http response
        """
        api_call_headers = {'Authorization': 'Bearer ' + self._get_token()}
        return requests.get(endpoint, headers=api_call_headers)
    
    def get_pipeline_run(self, run_id: str):
        return self._web_request(
            f"https://{self.workspace_name}.dev.azuresynapse.net/pipelineruns/{run_id}?api-version=2020-12-01",
        ).json()
    
    def get_activities_for_pipeline(self, pipeline_name: str, run_id: str):
        api_call_headers = {'Authorization': 'Bearer ' + self._get_token()}
        return requests.post(
            f"https://{self.workspace_name}.dev.azuresynapse.net/pipelines/{pipeline_name}/pipelineruns/{run_id}/queryActivityruns?api-version=2020-12-01",
            json={
                "lastUpdatedAfter": "2025-10-15T14:09:32.512Z",
                "lastUpdatedBefore": "2030-10-22T02:15:45.7054869Z"
            },
            headers=api_call_headers
        ).json()["value"]
    
    def get_child_pipeline_runs(self, pipeline_name: str, run_id: str):
        all_activities = self.get_activities_for_pipeline(pipeline_name, run_id)
        return [x for x in all_activities if x["activityType"] == "ExecutePipeline"]
    
    def get_notebook_calls(self, pipeline_name: str, run_id: str):
        all_activities = self.get_activities_for_pipeline(pipeline_name, run_id)
        return [x for x in all_activities if x["activityType"] == "SynapseNotebook"]
    
    def get_all_notebook_calls_for_pipeline(self, pipeline_name: str, pipeline_guid: str):
        child_pipelines_to_evaluate = self.get_child_pipeline_runs(pipeline_name, pipeline_guid)
        base_notebook_calls = self.get_notebook_calls(pipeline_name, pipeline_guid)
        notebook_call_map = {
            pipeline_name: base_notebook_calls
        }
        while child_pipelines_to_evaluate:
            next_pipeline = child_pipelines_to_evaluate.pop()
            next_pipeline_name = next_pipeline["output"]["pipelineName"]
            next_pipeline_run_id = next_pipeline["output"]["pipelineRunId"]
            new_child_pipelines = self.get_child_pipeline_runs(next_pipeline_name, next_pipeline_run_id)
            child_pipelines_to_evaluate += new_child_pipelines
            new_child_notebooks = self.get_notebook_calls(next_pipeline_name, next_pipeline_run_id)
            for notebook_call in new_child_notebooks:
                if next_pipeline_name in notebook_call_map:
                    notebook_call_map[next_pipeline_name].append(notebook_call)
                else:
                    notebook_call_map[next_pipeline_name] = [notebook_call]
        return notebook_call_map
    
    def get_relevant_notebook_calls_for_pipeline(self, pipeline_name: str, pipeline_guid: str):
        relevant_notebooks = self.get_all_unarchived_files_that_use_py_create_spark_schema()
        all_notebook_calls = self.get_all_notebook_calls_for_pipeline(pipeline_name, pipeline_guid)
        relevant_notebook_calls = {
            parent_pipeline: [
                notebook
                for notebook in notebooks
                if notebook["input"]["notebook"]["referenceName"] in relevant_notebooks
            ]
            for parent_pipeline, notebooks in all_notebook_calls.items()
        }
        return {
            k: v
            for k, v in relevant_notebook_calls.items()
            if v
        }

env = "dev"
#notebook_call_map = TableArchiveUtil(f"pins-synw-odw-{env}-uks").get_relevant_notebook_calls_for_pipeline("pln_master", "8141402c-de8e-4750-a845-2a8032cce37f")
with open("pipelines/scripts/samples.json", "r") as f:
    notebook_call_map = json.load(f)
notebook_call_map_cleaned = {
    k: [x["input"]["notebook"]["referenceName"] for x in v]
    for k, v in notebook_call_map.items()
}
print(json.dumps(notebook_call_map_cleaned, indent=4))
