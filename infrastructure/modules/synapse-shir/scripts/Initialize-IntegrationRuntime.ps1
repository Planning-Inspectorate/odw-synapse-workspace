Param(
  [Parameter(Mandatory=$true)]
  $AuthKey,

  [Parameter(Mandatory=$false)]
  $Path = 'C:\SHIR'
)

Function Assert-IntegrationRuntimeInstalled {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$HivePath = "HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall"
  )

  $InstalledSoftware = Get-ChildItem -Path $HivePath
  Foreach ($Package in $InstalledSoftware) {
    If (
      ($Package.GetValue("DisplayName") -eq "Microsoft Integration Runtime") -or
      ($Package.GetValue("DisplayName") -eq "Microsoft Integration Runtime Preview")
    ) {
      Return $true

    } Else {
      Return $false
    }
  }
}

Function Find-IntegrationRuntimeExecutable {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$HivePath = "HKLM:\Software\Microsoft\DataTransfer\DataManagementGateway\ConfigurationManager",

    [Parameter(Mandatory=$false)]
    [String]$RegistryKey = "DiacmdPath"
  )

  $ExecutablePath = Get-ItemPropertyValue -Path $HivePath -Name $RegistryKey
  If ([String]::IsNullOrEmpty($ExecutablePath)) {
    Throw "Cannot find Integration Runetime executable."

  } Else {
    Return (Split-Path -Parent $ExecutablePath) + "\dmgcmd.exe"
  }
}

Function Get-IntegrationRuntimeMsi {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$MsiPath = 'C:\SHIR',

    [Parameter(Mandatory=$false)]
    [String]$IntegrationRuntimeUri = 'https://go.microsoft.com/fwlink/?linkid=839822&clcid=0x409'
  )

  $MsiFiles = (Get-ChildItem -Path $Path | Where-Object {
    $_.Name -match [Regex] "IntegrationRuntime.*.msi"
  })

  If ($MsiFiles) {
    $MsiFileName = $MsiFiles[0].Name

  } Else {
    $MsiFileName = 'IntegrationRuntime.latest.msi'

    $ProgressPreference = 'SilentlyContinue'
    Invoke-WebRequest -Uri $IntegrationRuntimeUri -OutFile "$Path\$MsiFileName"
    $ProgressPreference = 'Continue'
  }

  Return "$Path\$MsiFileName"
}

Function Install-IntegrationRuntime {
  Param(
    [Parameter(Mandatory=$false)]
    [String]$MsiPath = 'C:\SHIR'
  )

  Uninstall-IntegrationRuntime

  $Install = Start-Process "msiexec.exe" "/i $MsiPath /quiet /passive" -Wait -PassThru
  If ($Install.ExitCode -ne 0) {
    Throw "Failed to install Integration Runtime: $($Install.ExitCode)"

  } Else {
    Start-Sleep 30
    Return
  }
}

Function Register-IntegrationRuntime {
  Param(
    [Parameter(Mandatory=$true)]
    [String]$AuthKey
  )

  $Executable = Find-IntegrationRuntimeExecutable

  $Registration = Start-Process $Executable "-k $AuthKey" -Wait -PassThru -NoNewWindow
  If ($Registration.ExitCode -ne 0) {
    Throw "Failed to register Integration Runtime: $($Install.ExitCode)"
  }
}

Function Uninstall-IntegrationRuntime {
  If (Assert-IntegrationRuntimeInstalled -eq $true) {
    [Void](Get-WmiObject -Class Win32_Product -Filter "Name='Microsoft Integration Runtime Preview' or Name='Microsoft Integration Runtime'" -ComputerName $env:COMPUTERNAME).Uninstall()
  }

  Return
}

# Create the working directory
New-Item -Path $Path -ItemType 'Directory' -ErrorAction 'SilentlyContinue'

# Find or download Integration Runtime MSI package
$Msi = Get-IntegrationRuntimeMsi -MsiPath $Path

# Install the Integration Runtime MSI package
Install-IntegrationRuntime -MsiPath $Msi

# Register the Integration Runtime with Synapse
Register-IntegrationRuntime -AuthKey $AuthKey
