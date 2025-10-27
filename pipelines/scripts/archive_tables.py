from tests.util.synapse_util import SynapseUtil
from azure.identity import AzureCliCredential
from typing import Dict, Any, Type, List
import requests
import json
import os


"""
This module identifies tables that are not referenced by the master pipelines

# ASSUMPTIONS
This works on the below assumptions
- The notebook `py_create_spark_schema` is used to create tables
- Tables may be referenced via loose code in notebooks/pipelines (i.e. using Pyspark/SQL directly)

# HOW IT WORKS
An analysis of the tables is done through the below process
1. Find all UNARCHIVED notebooks that reference `py_create_spark_schema` either directly or transitively
2. For a given pipeline run, find all "calls" of `py_create_spark_schema`
3. For each "call" of `py_create_spark_schema`, work out the inputs to the notebook. (Step 2 includes the parameters for each call)
   - A number of `SparkSchemaCallAnalyser` classes have been implemented to handle this logic for each calling notebook
4. Fetch all tables. We need to keep the tables from step 3. The notebooks not found in step 3 are archive candidates
5. Run an additional pass to check the archive candidate references against the output of step 1, incase these tables are
   referenced directly
6. Output is returned in the form
   ```
   {
       "tables_to_keep": [],  # Tables that satisfy either above assumptions
       "tables_to_arcive": []  # Tables that do no satisfy any of the above assumptions
   }
   ```

# USAGE
python3 pipelines/scripts/archive_tables.py -e dev --pln_master_run_id xxxx --pln_saphr_master_run_id yyyy

# NOTES
The output is not guaranteed to be correct, and mostly works on the defined assumptions due to the complexity of code analysis.
If you are using this to remove unused data, please double check the output beforehand
"""


class SparkSchemaCallAnalyser():
    """
    Class for identifying the usage of `py_create_spark_schema`
    """
    @classmethod
    def get_notebook(cls) -> str:
        return "create_table_from_schema"

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]) -> Dict[str, str]:
        """
        The inputs are a dictionary with the structure

        ```
        {
            "db_name": "",  # For some lake database
            "entity_name": ""  # For some table
        }
        ```
        """
        notebook_inputs = activity_run["input"]
        return {
            k: v["value"]
            for k, v in notebook_inputs["parameters"].items()
        }



class DartAPICallAnalyser(SparkSchemaCallAnalyser):
    @classmethod
    def get_notebook(cls) -> str:
        return "dart_api"

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]):
        return None


class AppealsFolderCuratedAnalyser(SparkSchemaCallAnalyser):
    @classmethod
    def get_notebook(cls) -> str:
        return "appeals_folder_curated"

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]):
        return None


class PySBRawToStdAnalyser(SparkSchemaCallAnalyser):
    """
    Class for identifying how `py_create_spark_schema` is used by calls to `py_sb_raw_to_std`
    """
    @classmethod
    def get_notebook(cls) -> str:
        return "py_sb_raw_to_std"

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]):
        notebook_inputs = activity_run["input"]
        args = {
            k: v["value"]
            for k, v in notebook_inputs["parameters"].items()
        }
        entity_name = args["entity_name"]
        return {"db_name": 'odw_standardised_db', "entity_name": entity_name}


class PyRawToStdAnalyser(SparkSchemaCallAnalyser):
    @classmethod
    def get_notebook(cls) -> str:
        return "py_raw_to_std"

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]):
        return None


class SparkSchemaCallAnalyserFactory():
    """
    Class to dynamically generate instances of `SparkSchemaCallAnalyser`
    """
    NOTEBOOK_ANALYSERS: List[SparkSchemaCallAnalyser] = [
        SparkSchemaCallAnalyser,
        DartAPICallAnalyser,
        AppealsFolderCuratedAnalyser,
        PySBRawToStdAnalyser,
        PyRawToStdAnalyser
    ]

    @classmethod
    def get(cls, notebook_name: str) -> Type[SparkSchemaCallAnalyser]:
        analyser_map = {
            analyser_class.get_notebook(): analyser_class
            for analyser_class in cls.NOTEBOOK_ANALYSERS
        }
        if notebook_name not in analyser_map:
            raise ValueError(f"No Analyser for notebook '{notebook_name}'")
        return analyser_map[notebook_name]


class TableArchiveUtil():
    """
    Class that contains functionality to detect tables to archive
    """
    def __init__(self, workspace_name: str, env: str):
        self.workspace_name = workspace_name
        self.env = env

    credential = None
    _token = None

    def _get_unarchived_artifacts(self, artifact_directory: str):
        """
        For the given artifact directory, return all artifact names that do not have the word `archive` in their
        underlying `folder.name` property
        """
        all_notebook_files = os.listdir(artifact_directory)
        file_content_map = {
            file: json.load(open(f"{artifact_directory}/{file}", "r"))
            for file in all_notebook_files
        }
        unarchived = {
            file.replace(".json", ""): content
            for file, content in file_content_map.items()
            if "archive" not in content["properties"].get("folder", dict()).get("name", "")
        }
        return set(unarchived.keys())

    def get_all_unarchived_notebooks(self):
        """
        Return all notebooks that have not been archived
        """
        return self._get_unarchived_artifacts("workspace/notebook")

    def get_all_unarchived_pipelines(self):
        """
        Return all pipelines that have not been archived
        """
        return self._get_unarchived_artifacts("workspace/pipeline")

    def get_all_unarchived_files_that_use_py_create_spark_schema(self):
        """
        Return all unachived notebooks that contain a reference to the `py_create_spark_schema` notebook. This also
        includes transitive relationships (i.e. notebooks that do not directly call `py_create_spark_schema`, but call
        another notebook that does)
        """
        unachived_notebooks = self.get_all_unarchived_notebooks()
        notebook_content_map = {
            file: open(f"workspace/notebook/{file}.json", "r").read()
            for file in unachived_notebooks
        }
        relationships = {"py_create_spark_schema"}
        all_notebooks_with_references = {"py_create_spark_schema"}
        # Perform a search until there are no more references to explore
        while relationships:
            next_relationship = relationships.pop()
            new_relationships = {
                file
                for file in unachived_notebooks
                if next_relationship in notebook_content_map[file]
            }
            new_relationships_to_explore = new_relationships.difference(all_notebooks_with_references)
            all_notebooks_with_references |= new_relationships
            relationships |= new_relationships_to_explore
        return all_notebooks_with_references

    @classmethod
    def _get_token(cls) -> str:
        """
        Get an auth token to Synapse
        """
        if not (cls.credential and cls._token):
            cls.credential = AzureCliCredential()
            cls._token = cls.credential.get_token("https://dev.azuresynapse.net").token
        return cls._token

    def get_activities_for_pipeline(self, pipeline_name: str, run_id: str) -> List[Dict[str, Any]]:
        """
        Return all activities of the given pipeline run
        """
        api_call_headers = {'Authorization': 'Bearer ' + self._get_token()}
        url = f"https://{self.workspace_name}.dev.azuresynapse.net/pipelines/{pipeline_name}/pipelineruns/{run_id}/queryActivityruns?api-version=2020-12-01"
        resp = requests.post(
            url,
            json={
                "lastUpdatedAfter": "2025-10-15T14:09:32.512Z",
                "lastUpdatedBefore": "2030-10-22T02:15:45.7054869Z"
            },
            headers=api_call_headers
        ).json()
        if "value" not in resp:
            raise RuntimeError(f"The response from azure when hitting '{url}' had an unexpected structure: {resp}")
        return resp["value"]

    def get_child_pipeline_runs(self, pipeline_name: str, run_id: str):
        """
        Return all child pipeline activities of the given pipeline run
        """
        all_activities = self.get_activities_for_pipeline(pipeline_name, run_id)
        return [x for x in all_activities if x["activityType"] == "ExecutePipeline"]

    def get_notebook_calls(self, pipeline_name: str, run_id: str):
        """
        Return all child notebook activities of the given pipeline run
        """
        all_activities = self.get_activities_for_pipeline(pipeline_name, run_id)
        return [x for x in all_activities if x["activityType"] == "SynapseNotebook"]

    def get_all_notebook_calls_for_pipeline(self, pipeline_name: str, pipeline_guid: str):
        """
        Deeply return all notebook calls of the given pipeline run

        :return: A dict of the form <pipeline_name: [notebook_call_json]>
        """
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
        """
        Deeply return all notebook calls for the given pipeline run that either directly or indirectly call the
        `py_create_spark_schema` notebook.

        :return: A dict of the form <pipeline_name: [notebook_call_json]>
        """
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

    def get_tables_referenced_by_notebooks(self, notebook_call_map: Dict[str, Dict[str, Any]]):
        """
        Return all tables referred to by notebooks

        :param notebook_call_map: A dictionary of the form <pipeline_name: [notebook_call] (i.e. the output of 
        `get_relevant_notebook_calls_for_pipeline`)
        :return: A list of the form [{entity_name: "", entity_name: ""}] (i.e. the list of input params for the
        `py_create_spark_schema` notebook)
        """
        notebook_calls = [
            x
            for y in notebook_call_map.values()
            for x in y
        ]
        referenced_tables = [
            SparkSchemaCallAnalyserFactory.get(
                notebook_call["input"]["notebook"]["referenceName"]
            )().get_inputs_to_py_create_spark_schema(notebook_call)
            for notebook_call in notebook_calls
        ]
        return {
            f"{x['db_name']}.{x['entity_name']}".replace("-", "_")  # Synapse converts these to underscores
            for x in referenced_tables
            if x
        }

    def get_tables_referenced_by_code(self, tables_to_archive: List[str]):
        """
        :param tables_to_archive: A list of tables to apply a filter on
        :return: The tables that are referenced by notebooks/pipelines
        """
        unarchived_notebooks = self.get_all_unarchived_notebooks()
        unarchived_pipelines = self.get_all_unarchived_pipelines()
        artifact_map = {
            notebook: open(f"workspace/notebook/{notebook}.json").read()
            for notebook in unarchived_notebooks
        }
        artifact_map |= {
            pipeline: open(f"workspace/pipeline/{pipeline}.json").read()
            for pipeline in unarchived_pipelines
        }
        return {
            table_path
            for table_path in tables_to_archive
            if any(
                # If the whole table name, or both parts of the table name are specified in the artifact
                table_path in artifact or (table_path.split(".")[1] in artifact and table_path.split(".")[1] in artifact)
                for artifact in artifact_map.values()
            )
        }

    def get_all_tables(self):
        """
        Return all tables in the form `database.tablename`
        """
        query = """
            select concat(TABLE_CATALOG, '.', table_name)
            from INFORMATION_SCHEMA.TABLES
            where TABLE_CATALOG = 'odw_curated_db' or TABLE_CATALOG = 'odw_harmonised_db' or TABLE_CATALOG = 'odw_standardised_db'
            order by TABLE_CATALOG, table_name
        """
        # Unfortunately we cannot run this from the master database - the query must be run from each database individually
        databases_to_query = ["odw_standardised_db", "odw_harmonised_db", "odw_curated_db"]
        all_tables = set()
        for database in databases_to_query:
            with SynapseUtil.get_pool_connection(database) as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                all_tables = all_tables.union([row[0] for row in cursor.fetchall()])
        return all_tables

    def get_table_analysis_result(self, master_pipeline_run_id: str, saphr_master_pipeline_run_id: str):
        """
        Generates a list of tables to archive and a list of tables to keep, in json format
        """
        notebook_call_map = self.get_relevant_notebook_calls_for_pipeline("pln_master", master_pipeline_run_id)
        notebook_call_map = notebook_call_map | self.get_relevant_notebook_calls_for_pipeline(
            "pln_saphr_master",
            saphr_master_pipeline_run_id
        )
        tables_to_keep = self.get_tables_referenced_by_notebooks(notebook_call_map)
        all_tables = self.get_all_tables()
        tables_to_archive = all_tables.difference(tables_to_keep)
        tables_referenced_by_artifacts = self.get_tables_referenced_by_code(tables_to_archive)
        tables_to_keep |= tables_referenced_by_artifacts
        tables_to_archive = tables_to_archive.difference(tables_referenced_by_artifacts)
        return {
            "tables_to_keep": sorted(list(tables_to_keep)),
            "tables_to_archive": sorted(list(tables_to_archive))
        }


if __name__ == "__main__":
    env = "dev"
    analysis_result = TableArchiveUtil(f"pins-synw-odw-{env}-uks", env).get_table_analysis_result(
        "8141402c-de8e-4750-a845-2a8032cce37f",
        "4522aad8-a0ff-402e-83ae-fc66f45a21c6"
    )
    print(json.dumps(analysis_result, indent=4))

