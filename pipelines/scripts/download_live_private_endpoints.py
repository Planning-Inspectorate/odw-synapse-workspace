from pipelines.scripts.synapse_artifact.synapse_managed_private_endpoint_util import SynapseManagedPrivateEndpointUtil
import argparse
import json
import os


"""
Download the live managed private endpoint artifacts from the workspace, and write them locally.
This is needed to allow the Synapse deployment to work with the below options
- DeleteArtifactsNotInTemplate: true
- DeployManagedPrivateEndpoints: false
"""
if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-e", "--env", required=True, help="The environment to target")
    args = parser.parse_args()
    env = args.env
    synapse_workspace = f"pins-synw-odw-{env}-uks"
    all_endpoints = SynapseManagedPrivateEndpointUtil(synapse_workspace).get_all(vnet="default")
    private_endpoint_names = {endpoint["name"]: endpoint for endpoint in all_endpoints}
    private_endpoint_directory = "workspace/managedVirtualNetwork/default/managedPrivateEndpoint"
    if not os.path.exists(private_endpoint_directory):
        os.makedirs(private_endpoint_directory)
    for endpoint_name, endpoint in private_endpoint_names.items():
        with open(f"{private_endpoint_directory}/{endpoint_name}.json", "w", encoding="utf-8") as f:
            json.dump(endpoint, f, indent="\t", ensure_ascii=False, sort_keys=True)
