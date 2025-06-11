from tests.util.synapse_test_case import SynapseTestCase
from typing import Dict, Any
import uuid
import requests
import json
import time
import logging


class NotebookRunException(Exception):
    pass

class NotebookWaitException(Exception):
    pass


class NotebookRunTestCase(SynapseTestCase):
    """
        Generic Test Case to handle running tests defined in Azure Synapse
    """
    def _trigger_notebook(self, notebook_name: str, notebook_parameters: Dict[str, Any]) -> str:
        """
            Trigger a synapse notebook run
        """
        logging.info(f"RUNNING notebook {notebook_name}...\n")
        notebook_run_id = str(uuid.uuid4())
        run_notebook_url = f'{self.SYNAPSE_ENDPOINT}/notebooks/runs/{notebook_run_id}?api-version=2022-03-01-preview'
        headers = {"Authorization": f"Bearer {self.SYNAPSE_ACCESS_TOKEN}", "Content-Type": "application/json"}
        response = requests.put(run_notebook_url, headers=headers,data=json.dumps(notebook_parameters))
        if response.status_code >= 200 and response.status_code < 400 : 
            return notebook_run_id
        raise NotebookRunException(f"Failed to run notebook {notebook_name}. RunID: '{notebook_run_id}'. Error is {response.status_code}...\n")

    def _wait_for_notebook_run(self, notebook_run_id: str, poll_interval = 15, max_wait_time_minutes: int = 10) -> str:
        """
            Wait for a synapse notebook run to finish
        """
        current_wait_time = 0
        max_wait_time_seconds = max_wait_time_minutes * 60
        notebook_run_end_states = {"Succeeded", "TimedOut", "Failed", "Cancelled"}
        run_notebook_url = f"{self.SYNAPSE_ENDPOINT}/notebooks/runs/{notebook_run_id}?api-version=2022-03-01-preview"
        while current_wait_time < max_wait_time_seconds:
            logging.info(f"Waiting for the notebook run id '{notebook_run_id}' exist state to be one of {notebook_run_end_states}")
            headers = {"Authorization": f"Bearer {self.SYNAPSE_ACCESS_TOKEN}", "Content-Type": "application/json"}
            response = requests.get(run_notebook_url, headers=headers)
            if response.status_code >= 200 and response.status_code < 400: 
                notebook_run_status = response.json()["result"]["runStatus"]
                if notebook_run_status in notebook_run_end_states:
                    return notebook_run_status
            else:
                raise NotebookWaitException(f"Notebook poll request raised a status code {response.status_code}")
            current_wait_time += poll_interval
            time.sleep(poll_interval)
        return NotebookWaitException(f"Exceeded max wait time for the test notebook run with id {notebook_run_id} of {max_wait_time_minutes} minutes")

    def run_notebook(self, notebook_name: str, notebook_parameters: Dict[str, Any], max_wait_time_minutes: int = 10):
        notebook_run_id = self._trigger_notebook(notebook_name, notebook_parameters)
        notebook_run_status = self._wait_for_notebook_run(notebook_run_id, max_wait_time_minutes)
        assert notebook_run_status == "Succeeded"
