# ==============================================================================
# Script       : Install-OpenJDK.ps1
# Description  : Installs Adoptium's OpenJDK 8 for x64 Windows
# Version      : 1.0.0
# Author       : Lester March (https://github.com/lestermarch)
# Help         : https://adoptium.net/temurin/archive/?version=8
# ==============================================================================

Param(
  [Parameter(Mandatory=$true)]
  $DownloadUri,

  [Parameter(Mandatory=$true)]
  $Path
)

Function Get-OpenJdkMsi {
  Param(
    [Parameter(Mandatory=$true)]
    [String]$MsiPath,

    [Parameter(Mandatory=$true)]
    [String]$OpenJdkUri
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
    [Parameter(Mandatory=$true)]
    [String]$MsiPath
  )

  $Install = Start-Process "msiexec.exe" "/i $MsiPath /quiet /passive" -Wait -PassThru
  If ($Install.ExitCode -ne 0) {
    Throw "Failed to install OpenJDK: $($Install.ExitCode)"

  } Else {
    Start-Sleep 30
    Return
  }
}

Function Set-JavaEnvironmentVariables {
  Try {
    $JdkWmi = Get-WmiObject -Class Win32_Product | Where-Object { $_.Name -like "*JDK*" } -ErrorAction 'Stop'
    If ($JdkWmi.Count -gt 1) {
      $JdkPath = $JdkWmi[0].InstallLocation

    } Else {
      $JdkPath = $JdkWmi.InstallLocation
    }

    [Environment]::SetEnvironmentVariable("JAVA_HOME", $JdkPath, 'Machine')
    [Environment]::SetEnvironmentVariable("JRE_HOME", $JdkPath, 'Machine')

  } Catch {
    Throw "Failed to find OpenJDK install location"
  }
}

# Create the working directory
New-Item -Path $Path -ItemType 'Directory' -ErrorAction 'SilentlyContinue'

# Find or download the Integration Runtime MSI package
$Msi = Get-OpenJdkMsi -MsiPath $Path -OpenJdkUri $DownloadUri

# Install the Integration Runtime MSI package
Install-OpenJdk -MsiPath $Msi

# Set JAVA_HOME and JRE_HOME environment variables
Set-JavaEnvironmentVariables
