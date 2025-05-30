import requests
import datetime
import time
import json
from azure.synapse.artifacts import ArtifactsClient
from azure.identity import ClientSecretCredential
import uuid
import pprint


def list_pipelines(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str):
    print(f'Listing pipelines...\n')
    # Implementation with REST API
    run_pipeline_url = f'{synapse_endpoint}/pipelines?api-version=2020-12-01'
    access_token = azure_credential.get_token(credential_name)
    headers = {'Authorization': f'Bearer {access_token.token}', 'Content-Type': 'application/json'}
    response = requests.get(run_pipeline_url, headers=headers)

    if response.status_code == 200 : 
        print(response.content)
    else:
        print(f'Failed to list pipelines, Error is {response.status_code}...\n')


def run_and_observe_pipeline(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str, pipeline_name: str,params: dict):
    synapse_client: ArtifactsClient = ArtifactsClient(azure_credential, synapse_endpoint)
    if params is not None:
        success, pipeline_run_id = _run_pipeline(credential_name, azure_credential, synapse_endpoint, pipeline_name, params)
    else:
        success, pipeline_run_id = _run_pipeline(credential_name, azure_credential, synapse_endpoint, pipeline_name)
    
    if (success):
        print(f'PIPELINE RUNNING WITH RunID: {pipeline_run_id}\n')
        pipeline_run_status, exitMessage = observe_pipeline(credential_name, azure_credential, synapse_endpoint, pipeline_run_id)
        return pipeline_run_status
    else:
        print(f'PIPELINE NOT RUNNING FOR RunID: {pipeline_run_id}\n')
        return "Failed"

def _run_pipeline(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str, pipeline_name: str, params: dict) -> str:
    print(f'RUNNING PIPELINE {pipeline_name}...\n')
    # Implementation with REST API
    run_pipeline_url = f'{synapse_endpoint}/pipelines/{pipeline_name}/createRun?api-version=2020-12-01'
    access_token = azure_credential.get_token(credential_name)
    headers = {'Authorization': f'Bearer {access_token.token}', 'Content-Type': 'application/json'}
    response = requests.post(run_pipeline_url, headers=headers,data=json.dumps(params))

    if response.status_code >= 200 and response.status_code < 400 : 
        pipeline_run_id = response.json()['runId']
        print(response.content)
        return (True, pipeline_run_id)
    else:
        print(f'Failed to run pipeline {pipeline_name}, Error is {response.status_code}...\n')
        return (False, "NONE")

def observe_pipeline(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str, run_id: str, until_status=["Succeeded", 
                    "TimedOut","Failed", "Cancelled"],poll_interval=15) -> str:
    print('OBSERVING PIPELINE RUN...\n')
    pipeline_run_status = ""
    while(pipeline_run_status not in until_status):
        now = datetime.datetime.now()
        print( f'{now.strftime("%Y-%m-%d %H:%M:%S")}' f' Polling pipeline with run id {run_id}'f' for status in {", ".join(until_status)}')
        
        run_pipeline_url = f'{synapse_endpoint}/pipelineruns/{run_id}?api-version=2020-12-01'
        print("Status check URL is " +run_pipeline_url)
        while(pipeline_run_status not in until_status):
            now = datetime.datetime.now()
            print(f'{now.strftime("%Y-%m-%d %H:%M:%S")}'f' Polling pipeline with run id {run_id}'f' for status in {", ".join(until_status)}')
            access_token = azure_credential.get_token(credential_name)
            headers = {'Authorization': f'Bearer {access_token.token}', 'Content-Type': 'application/json'}
            response = requests.get(run_pipeline_url, headers=headers)
            if response.status_code >= 200 and response.status_code < 400: 
                print(response.content)
                pipeline_run_status = response.json()['status']
                exitMessage = response.json()['message']
                time.sleep(poll_interval)
            else:
                print("Failed to check status " +str(response.status_code))
                return ("Failed", "")
        time.sleep(poll_interval)
    print( f'pipeline run id {run_id}' f'finished with status {pipeline_run_status}\n')
    return (pipeline_run_status, exitMessage)

def run_and_observe_notebook(credential_name: str, azure_credential: ClientSecretCredential,synapse_endpoint: str, notebook_name: str,params: dict):
    synapse_client: ArtifactsClient = ArtifactsClient(azure_credential, synapse_endpoint)
    if params is not None:
        #print(f"{notebook_name} Notebook Raw Parameters : {params}\n")
        success, notebook_run_id = _run_notebook(credential_name, azure_credential, synapse_endpoint, notebook_name, params)
    else:
        success, notebook_run_id = _run_notebook(credential_name, azure_credential, synapse_endpoint, notebook_name)
    
    if (success):
        print(f'notebook RUNNING WITH RunID: {notebook_run_id}\n')
        notebook_run_status, exitMessage = observe_notebook(credential_name, azure_credential, synapse_endpoint, notebook_run_id)
        return (notebook_run_status, exitMessage)
    else:
        print(f'notebook NOT RUNNING FOR RunID: {notebook_run_id}\n')
        return ("Failed", "Execution Error")

def _run_notebook(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str, notebook_name: str,params: dict) -> str:
    print(f'RUNNING notebook {notebook_name}...\n')
    # Implementation with REST API
    notebook_run_id:str = str(uuid.uuid4())
    run_notebook_url = f'{synapse_endpoint}/notebooks/runs/{notebook_run_id}?api-version=2022-03-01-preview'
    access_token = azure_credential.get_token(credential_name)
    headers = {'Authorization': f'Bearer {access_token.token}', 'Content-Type': 'application/json'}
    response = requests.put(run_notebook_url, headers=headers,data=json.dumps(params))
    if response.status_code >= 200 and response.status_code < 400 : 
        print(response.content)
        return (True, notebook_run_id)
    else:
        print(f'Failed to run notebook {notebook_name}, Error is {response.status_code}...\n')
        return (False, notebook_run_id)
    


#Logs are available here : https://portal.azure.com/#view/Microsoft_Azure_Monitoring/AzureMonitoringBrowseBlade/~/logs
#query is 
#   search in (SynapseGatewayApiRequests) "<runid>" 

def observe_notebook(credential_name: str, azure_credential: ClientSecretCredential, synapse_endpoint: str,  notebook_run_id: str, until_status=["Succeeded", "TimedOut","Failed", "Cancelled"],poll_interval=15) -> str:
    print('OBSERVING notebook RUN...\n')
    notebook_run_status = ""
    run_notebook_url = f'{synapse_endpoint}/notebooks/runs/{notebook_run_id}?api-version=2022-03-01-preview'
    print("Status check URL is " +run_notebook_url)
    while(notebook_run_status not in until_status):
        now = datetime.datetime.now()
        print(f'{now.strftime("%Y-%m-%d %H:%M:%S")}'f' Polling notebook with run id {notebook_run_id}'f' for status in {", ".join(until_status)}')
        access_token = azure_credential.get_token(credential_name)
        headers = {'Authorization': f'Bearer {access_token.token}', 'Content-Type': 'application/json'}
        response = requests.get(run_notebook_url, headers=headers)
        if response.status_code >= 200 and response.status_code < 400: 
            print(response.content)
            notebook_run_status = response.json()['result']['runStatus']
            exitMessage = response.json()['result']['exitValue']
            time.sleep(poll_interval)
        else:
            print("Failed to check status " +str(response.status_code))
            return ("Failed", "")

    print(f'notebook run id {notebook_run_id}' f'finished with status {notebook_run_status}\n')
    return (notebook_run_status, exitMessage)