from pipelines.scripts.synapse_artifact.synapse_managed_private_endpoint_util import SynapseManagedPrivateEndpointUtil
import argparse
import json


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
    for endpoint_name, endpoint in private_endpoint_names.items():
        with open(f"workspace/managedVirtualNetwork/default/managedPrivateEndpoint/{endpoint_name}.json", "w") as f:
            json.dump(endpoint, f, indent="\t", ensure_ascii=False)
