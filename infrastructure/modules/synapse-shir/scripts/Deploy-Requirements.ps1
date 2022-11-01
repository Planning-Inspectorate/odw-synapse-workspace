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

# Download, install and register the Microsoft Integration Runtime
$ParamInitIntegrationRuntime = @{
  AuthKey     = $AuthKey
  DownloadUri = 'https://go.microsoft.com/fwlink/?linkid=839822&clcid=0x409'
  Path        = 'C:\Temp\SHIR'
}

& .\Initialize-IntegrationRuntime.ps1 @ParamInitIntegrationRuntime

# Download and install Java Development Kit & Runtime Environment
$ParamInstallOpenJdk = @{
  DownloadUri = 'https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u345-b01/OpenJDK8U-jdk_x64_windows_hotspot_8u345b01.msi'
  Path        = 'C:\Temp\OpenJDK'
}

& .\Install-OpenJDK.ps1 @ParamInstallOpenJdk
