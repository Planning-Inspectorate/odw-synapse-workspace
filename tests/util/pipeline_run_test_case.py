from tests.util.synapse_test_case import SynapseTestCase
from typing import Dict, Any
import requests
import json
import time
import logging


class PipelineRunException(Exception):
    pass

class PipelineWaitException(Exception):
    pass


class PipelineRunTestCase(SynapseTestCase):
    def _trigger_pipeline(self, pipeline_name: str, pipeline_parameters: Dict[str, Any]) -> str:
        """
            GenericTrigger a synapse pipeline run
        """
        logging.info(f"RUNNING pipeline {pipeline_name}...\n")
        run_pipeline_url = f'{self.SYNAPSE_ENDPOINT}/pipelines/{pipeline_name}/createRun?api-version=2020-12-01'
        headers = {"Authorization": f"Bearer {self.SYNAPSE_ACCESS_TOKEN}", "Content-Type": "application/json"}
        response = requests.post(run_pipeline_url, headers=headers,data=json.dumps(pipeline_parameters))
        if response.status_code >= 200 and response.status_code < 400 :
            return response.json()['runId']
        raise PipelineRunException(f"Failed to run pipeline {pipeline_name}. Error is {response.status_code}...\n")

    def _wait_for_pipeline_run(self, pipeline_run_id: str, poll_interval = 15, max_wait_time_minutes: int = 10) -> str:
        """
            Wait for a synapse pipeline run to finish
        """
        current_wait_time = 0
        max_wait_time_seconds = max_wait_time_minutes * 60
        pipeline_run_end_states = {"Succeeded", "TimedOut", "Failed", "Cancelled"}
        run_pipeline_url = f"{self.SYNAPSE_ENDPOINT}/pipelineruns/{pipeline_run_id}?api-version=2020-12-01"
        while current_wait_time < max_wait_time_seconds:
            logging.info(f"Waiting for the pipeline run id '{pipeline_run_id}' exist state to be one of {pipeline_run_end_states}")
            headers = {"Authorization": f"Bearer {self.SYNAPSE_ACCESS_TOKEN}", "Content-Type": "application/json"}
            response = requests.get(run_pipeline_url, headers=headers)
            if response.status_code >= 200 and response.status_code < 400:
                pipeline_run_json = response.json()
                pipeline_run_status = pipeline_run_json["status"]
                if pipeline_run_status in pipeline_run_end_states:
                    return pipeline_run_json
            else:
                raise PipelineWaitException(f"Pipeline poll request raised a status code {response.status_code}")
            current_wait_time += poll_interval
            time.sleep(poll_interval)
        raise PipelineWaitException(f"Exceeded max wait time for the test pipeline run with id {pipeline_run_id} of {max_wait_time_minutes} minutes")

    """
        Test Case to handle running tests defined in Azure Synapse
    """
    def run_pipeline(self, pipeline_name: str, pipeline_parameters: Dict[str, Any], max_wait_time_minutes: int = 10):
        pipeline_run_id = self._trigger_pipeline(pipeline_name, pipeline_parameters)
        pipeline_run_result = self._wait_for_pipeline_run(pipeline_run_id, max_wait_time_minutes=max_wait_time_minutes)
        assert pipeline_run_result["status"] == "Succeeded"
        return pipeline_run_result
