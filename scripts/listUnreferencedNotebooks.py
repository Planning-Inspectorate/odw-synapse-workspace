#pip install azure-mgmt-synapse azure-identity requests

import requests
import os
import re
import json
import jsonpath_rw_ext
from jsonpath_ng.ext import parse

from azure.identity import DefaultAzureCredential

import sys
sys.stdout = open('notebooksAudit.txt','wt')


# Replace these with your actual values
environment = "dev"
subscription_id = 'ff442a29-fc06-4a13-8e3e-65fd5da513b3'
resource_group_name = f'pins-rg-data-odw-{environment}-uks'
workspace_name = f'pins-synw-odw-{environment}-uks'

# Use DefaultAzureCredential to authenticate
credential = DefaultAzureCredential()

# Get an access token for the REST API
token = credential.get_token(f'https://dev.azuresynapse.net/.default').token

# Define the base URL for Synapse Workspace REST API
base_url = f"https://pins-synw-odw-{environment}-uks.dev.azuresynapse.net/"

total_referenced_notebooks_by_pipelines = 0
total_notebooks = 0
total_pipelines = 0
total_referenced_notebooks = 0
total_unreferenced_notebooks = 0

print(f"######### Running against {workspace_name} #########")

#get a list of all of the pipelines (paginated)
def read_paginated_data(url, headers):
    data = []
    while url:
        response = requests.get(url, headers=headers)
        response_json = response.json()
        data.extend(response_json.get('value', []))
        url = response_json.get('nextLink')
    return data

def read_paginated_data_post(url, data, headers):
    response_data = []
    continuationToken = ''
    while continuationToken is not None:
        #print(f"Reading from {url}")
        response = requests.post(url, json=data, headers=headers)
        response_json = response.json()
        response_data.extend(response_json.get('value', []))
        
        tokenData = response_json.get('continuationToken')
        if tokenData is not None:
            continuationToken = tokenData
            data['continuationToken'] = tokenData
        
        if tokenData is None:
            continuationToken = None
            break
        
    return response_data


def get_pipline_runs():
    runs_url = f'{base_url}queryPipelineRuns?api-version=2020-12-01'
    headers = {'Authorization': f'Bearer {token}'}
    data = {
        "lastUpdatedAfter": "2018-06-16T00:36:44.3345758Z",
        "lastUpdatedBefore": "2026-01-16T00:49:48.3686473Z",
        "orderBy": [
            {
                "order": "ASC",
                "orderBy": "RunEnd"
            }
        ]
    }
    runs = read_paginated_data_post(runs_url, data=data, headers=headers)
    
    pipeline_runs = {}

    if runs:
        for run in runs:
            pipeline_runs[run['pipelineName']] = run['runStart']

    else:
        raise Exception(f"Error retrieving pipelines")

    return pipeline_runs

# Function to get a list of notebooks
def get_notebooks():
    notebooks_url = f'{base_url}notebooks?api-version=2020-12-01'
    #print(f"Reading from {notebooks_url}")
    headers = {'Authorization': f'Bearer {token}'}
    notebooks = read_paginated_data(notebooks_url, headers=headers)
    
    if notebooks:
        return [notebook['name'] for notebook in notebooks]
    else:
        raise Exception(f"Error retrieving notebooks")


# Function to get all pipeline names and their notebook references
def get_pipeline_references(pipeline_runs):
    pipeline_references = set()
    pipeline_subreferences = set()
    pipelines_url = f'{base_url}pipelines?api-version=2020-12-01'
    headers = {'Authorization': f'Bearer {token}'}
    pipelines = read_paginated_data(pipelines_url, headers=headers)
    
    print("Reading pipelines and notebooks")
    if pipelines:
        for pipeline in pipelines:
            pipeline_name = pipeline['name']
            pipeline_definition_url = f'{base_url}pipelines/{pipeline_name}?api-version=2021-06-01'
            pipeline_def_response = requests.get(pipeline_definition_url, headers=headers)
            
            if pipeline_def_response.status_code == 200:
                pipeline_definition = pipeline_def_response.json()
                #print(pipeline_definition)
                print(f"{pipeline_name}")

                notebook_jsonpath_expr = parse('$..notebook.referenceName')
                notebook_matches = notebook_jsonpath_expr.find(pipeline_definition)

                pipeline_jsonpath_expr = parse('$..pipeline.referenceName')
                pipeline_matches = pipeline_jsonpath_expr.find(pipeline_definition)

                for pipelineMatch in pipeline_matches:
                    print(f"\t\tPIPELINE : {pipelineMatch.value}")
                    pipeline_subreferences.add(str(pipelineMatch.value))

                # Extract and print the matched elements
                for notebook in notebook_matches:
                    print(f"\t\tNOTEBOOK : {notebook.value}")
                    pipeline_references.add(str(notebook.value))
            else:
                print("FAILED TO READ PIPELINES")

        #sort the list of pipelines
        pipeline_list = []
        for pipeline in pipelines:
            pipeline_list.append(pipeline['name'])
        pipeline_list = sorted(pipeline_list)

        #sort the list of subpipelines
        subpipeline_list = sorted(pipeline_subreferences)  

        global total_pipelines
        total_pipelines = len(pipeline_list)

        print('**************** LIST OF PIPELINES *******************')
        for pipeline in pipeline_list:
            if pipeline in pipeline_runs:
                print(f"{pipeline}: {pipeline_runs[pipeline]}")
            else:
                print(f"{pipeline}: NONE")
        print('**************** END OF LIST OF PIPELINES *******************')  

        print('**************** LIST OF PIPELINES CALLED BY OTHER PIPELINES *******************')
        for subpipeline in subpipeline_list:
            if subpipeline in pipeline_runs:
                print(f"{subpipeline}: {pipeline_runs[subpipeline]}")
            else:
                print(f"{subpipeline}: NONE")
        print('**************** LIST OF PIPELINES CALLED BY OTHER PIPELINES *******************')  

    else:
        raise Exception(f"Error retrieving pipelines")
    
    return pipeline_references

# Main logic to find unreferenced notebooks
def find_unreferenced_notebooks(pipeline_runs):
    referenced_notebooks = get_pipeline_references(pipeline_runs)

    list_referenced_notebooks = sorted(list(set(referenced_notebooks)))
    global total_referenced_notebooks_by_pipelines
    total_referenced_notebooks_by_pipelines = len(list_referenced_notebooks)
    print("*********** LIST OF REFERENCED NOTEBOOKS ***********")
    for notebook in list_referenced_notebooks:
        print(notebook)
    print("*********** END OF LIST OF REFERENCED NOTEBOOKS ***********")

    notebooks = get_notebooks()
    global total_notebooks
    total_notebooks = len(notebooks)
    
    # Find notebooks that are not referenced by any pipeline
    unreferenced_notebooks = set(notebooks) - referenced_notebooks
    return sorted(unreferenced_notebooks)


def grep_files(pattern, pattern2, root_dir):
    # Compile the regular expression pattern
    regex = re.compile(pattern)
    regex2 = re.compile(pattern2)
    notebooks = set()
    
    # Walk through the directory
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            file_path = os.path.join(subdir, file)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        match = regex.search(line)
                        if (match is not None):
                            try:
                                data2 = regex2.search(match.group(1).strip())
                                if data2 is not None:
                                    data = data2.group(1)

                                    #quick tidy
                                    data = data.split(' ')[0].replace("\"", "").replace("\\", "").replace("'", "").split(',')[0].strip()
                                    notebooks.add(f"{data}")
                            except Exception:
                                print("No Matches")

            except (UnicodeDecodeError, FileNotFoundError):
                # Skip files that can't be read
                continue
    return notebooks

pipeline_runs = get_pipline_runs()
#for key in pipeline_runs.keys():
#    print(f"{key}: {pipeline_runs[key]}")

#get runs from source code
pattern = r'%run (.+)'
pattern2 = r'([^/]+)\"$'
root_dir = '../workspace/notebook/'
#get a list of the lines that start with pattern and extract them (with some tidying)
source_notebooks = grep_files(pattern, pattern2, root_dir)

#get runs from source code (with a different pattern)
pattern = r'.*mssparkutils.notebook.run\((.+)\)'
pattern2 = r'([^/]+)$'
root_dir = '../workspace/notebook/'
#get a list of the lines that start with pattern and extract them (with some tidying)
source_notebooks2 = grep_files(pattern, pattern2, root_dir)

print("*********** LIST OF NOTEBOOKS FROM RUN SOURCECODE***********")
for source_notebook in sorted(source_notebooks):
    print(source_notebook)
print("*********** END OF LIST OF NOTEBOOKS FROM RUN SOURCECODE ***********")    

print("*********** LIST OF NOTEBOOKS FROM RUN SOURCECODE2***********")
for source_notebook in sorted(source_notebooks2):
    print(source_notebook)
print("*********** END OF LIST OF NOTEBOOKS FROM RUN SOURCECODE2 ***********")    

#combine the two sets
all_source = source_notebooks.union(source_notebooks2)

# Get unreferenced notebooks
unreferenced_notebooks = find_unreferenced_notebooks(pipeline_runs)
unreferenced_notebooks = set(unreferenced_notebooks) - all_source

# Print out the unreferenced notebooks
print("*********** LIST OF UNREFERENCED NOTEBOOKS ***********")
for notebook in sorted(unreferenced_notebooks):
    print(notebook)
print("*********** END OF LIST OF UNREFERENCED NOTEBOOKS ***********")

print(f"Total pipelines {total_pipelines}")
print(f"Total notebooks {total_notebooks}")
print(f"Notebooks referenced by pipelines {total_referenced_notebooks_by_pipelines}")
print(f"Notebooks found in source check 1 {len(source_notebooks)}")
print(f"Notebooks found in source check 2 {len(source_notebooks2)}")
print(f"Notebooks found in combined set from source {len(all_source)}")