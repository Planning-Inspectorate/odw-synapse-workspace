# Configuration Files
The configuration files in this folder are used by the Terraform root module for deploying ODW infrastructure. Please see below details before updating any configuration files.

## Data Lifecycle
The `data-lifecycle` directory contains a `policies.json` file which describes the data lifecycle policies that should be applied to the Data Lake Storage Account. The policy file is formatted as a a list of JSON objects, for example:
```bash
[
  {
    "rule_name" : "example_rule",
    "prefix_match" : [
      "container1/folder1",
      "container1/folder2",
      "container2/folder1"
    ],
    "blob" : {
      "cool_tier_since_modified_days" : 30,
      "archive_tier_since_modified_days" : 60,
      "delete_since_modified_days" : 90
    },
    "snapshot" : {
      "cool_tier_since_created_days" : 30,
      "archive_tier_since_created_days" : 60,
      "delete_since_created_days" : 90
    },
    "version" : {
      "cool_tier_since_created_days" : 30,
      "archive_tier_since_created_days" : 60,
      "delete_since_created_days" : 90
    }
  }
]
```

For each of the `blob`, `snapshot`, and `version` sub-objects, attributes are optional. If any attribute is omitted from the policy then the action will not be performed. For example, omitting `archive_tier_since_modified_days` will mean that the respective data type will not be archieved but would be moved to cool storage and eventually deleted.

## DevOps Agents
The  `devops-agents` directory contains [Packer](https://www.packer.io/) files which describe the image used to provision the Azure DevOps agent pool within the ODW network. The `devops-agent-deploy.yaml` pipeline runs to bootstraps the ODW environment, build the agent image using the files in this directory, then builds a self-hosted agent pool (VM Scale Set). After deployment and registration with Azure DevOps, the pool is managed entirely by Azure DevOps.

The `build.pkr.hcl` file can be configured to specify the base image version. For example, it may be desireable to change the `image_sku` to a newer Ubuntu version:
```bash
os_type         = "Linux"
image_publisher = "canonical"
image_offer     = "0001-com-ubuntu-server-focal"
image_sku       = "20_04-lts"
```

The `tools.sh` file is a Bash script used during image creation to install packages required for deploying the platform as per the `terraform-cd.yaml` pipeline. Please ensure that you test image creation after editing this file. Note that PowerShell Core is also installed, so PowerShell modules may also be added if required:
```bash
# PowerShell Modules
pwsh -c "& {Install-Module -Name Az -Scope AllUsers -Repository PSGallery -Force -Verbose}"
pwsh -c "& {Get-Module -ListAvailable}"
```

## Firewall Rules
The `allowed_ip_addresses.yaml` file in this directory is a placeholder file. IP addresses are subject to GDPR restrictions and as such this file should **not** be updated here to include any IP addresses. To update the list of allowed IP addresses, navigate to the [ODW Azure DevOps project](https://dev.azure.com/planninginspectorate/operational-data-warehouse/_library?itemType=SecureFiles) and upload a new secure file with the following format (a YAML list containing a list of IP addresses or ranges with comments):

```yaml
- "192.168.10.10"     # Single addresses should not have a prefix
- "192.168.10.0/24"   # Ranges of addresses require a CIDR prefix
```

## Spark Pool
The `spark-requirements.txt` file in this directory is used as an input for the Synapse Workspace and defines which Python packages (and optionally, their version) should be installed and made available for use within the Spark pool environment. The format for this file is outlined below:

```bash
# Package with a specified version:
<packageName>==<versionNumber>
# Package with no specified version (latest):
<packageName>
```
