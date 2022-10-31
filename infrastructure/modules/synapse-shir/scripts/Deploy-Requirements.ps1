# ==============================================================================
# Script       : Deploy-Requirements.ps1
# Description  : Wrapper script for use with Custom Script Extension
# Version      : 1.0.0
# Author       : Lester March (https://github.com/lestermarch)
# Help         : https://learn.microsoft.com/en-us/azure/virtual-machines/extensions/custom-script-windows#using-multiple-scripts
# ==============================================================================

Param(
  [Parameter(Mandatory=$true)]
  $AuthKey
)

& .\Initialize-IntegrationRuntime.ps1 -AuthKey $AuthKey
& .\Install-OpenJDK.ps1
