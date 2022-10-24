# Configuration Files
The configuration files in this folder are used by the Terraform root module for deploying ODW infrastructure. Please see below details before updating any configuration files.

## DevOps Agents
The  `devops-agents` directory contains [Packer](https://www.packer.io/) files which describe the image used to provision the Azure DevOps agent pool within the ODW network. The `devops-agent-deploy.yaml` pipeline runs to bootstraps the ODW environment, build the agent image using the files in this directory, then builds a self-hosted agent pool (VM Scale Set). After deployment and registration with Azure DevOps, the pool is managed entirely by Azure DevOps.

The `build.pkr.hcl` file can be configured to specify the base image version. For example, it may be desireable to change the `image_sku` to a newer Ubuntu version:
```json
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

## Spark Pool
The `spark-requirements.txt` file in this directory is used as an input for the Synapse Workspace and defines which Python packages (and optionally, their version) should be installed and made available for use within the Spark pool environment. The format for this file is outlined below:

```
# Package with a specified version:
<packageName>==<versionNumber>
# Package with no specified version (latest):
<packageName>
```
