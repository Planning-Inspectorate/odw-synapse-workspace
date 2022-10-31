# ==============================================================================
# Script       : Install-OpenJDK.ps1
# Description  : Installs Adoptium's OpenJDK 8 for x64 Windows
# Version      : 1.0.0
# Author       : Lester March (https://github.com/lestermarch)
# Help         : https://adoptium.net/temurin/archive/?version=8
# ==============================================================================

Param(
  [Parameter(Mandatory=$false)]
  $Path = 'C:\OpenJDK'
)

Function Get-OpenJdkMsi {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$MsiPath = 'C:\OpenJDK',

    [Parameter(Mandatory=$false)]
    [String]$OpenJdkUri = "https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u345-b01/OpenJDK8U-jdk_x86-32_windows_hotspot_8u345b01.msi"
  )

  $MsiFileName = $OpenJdkUri.Split("/")[-1]

  $MsiFiles = (Get-ChildItem -Path $Path | Where-Object {
    $_.Name -match [Regex] $MsiFileName
  })

  If ($MsiFiles) {
    $MsiFileName = $MsiFiles[0].Name

  } Else {
    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $OpenJdkUri -OutFile "$Path\$MsiFileName"
    $ProgressPreference = 'Continue'
  }

  Return "$Path\$MsiFileName"
}

Function Install-OpenJdk {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$MsiPath = 'C:\OpenJDK'
  )

  $Install = Start-Process "msiexec.exe" "/i $MsiPath /quiet /passive" -Wait -PassThru
  If ($Install.ExitCode -ne 0) {
    Throw "Failed to install OpenJDK: $($Install.ExitCode)"

  } Else {
    Start-Sleep 30
    Return
  }
}

# Create the working directory
New-Item -Path $Path -ItemType 'Directory' -ErrorAction 'SilentlyContinue'

# Find or download the Integration Runtime MSI package
$Msi = Get-OpenJdkMsi -MsiPath $Path

# Install the Integration Runtime MSI package
Install-OpenJdk -MsiPath $Msi
