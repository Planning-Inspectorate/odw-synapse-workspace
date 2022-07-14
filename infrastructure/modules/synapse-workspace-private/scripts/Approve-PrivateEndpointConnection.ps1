[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]
  [String]$ResourceId,

  [Parameter(Mandatory=$false)]
  [String]$ApprovalDescription = "Approved with Terraform"
)

Try {
  $ResourceName = $ResourceId.Split("/")[-1]

  $EndTime = (Get-Date).AddSeconds(60)
  Write-Host "Checking private endpoint connections for $ResourceName"

  Do {
    $Endpoint = Get-AzPrivateEndpointConnection -PrivateLinkResourceId $ResourceId
    Start-Sleep -Seconds 10
  } Until (($null -ne $Endpoint) -or ((Get-Date) -gt $EndTime))

  If ($null -eq $Endpoint) {
    Throw "Failed to find private endpoint connection for $ResourceName"
  }

  $EndpointName   = $Endpoint.Name
  $EndpointStatus = $Endpoint.PrivateLinkServiceConnectionState.Status
  Write-Host "Found private endpoint connection $EndpointName with status $EndpointStatus"

  If ($EndpointStatus -eq "Approved") {
    Write-Host "Private endpoint connection $EndpointName is already approved"
    Exit 0

  } ElseIf ($EndpointStatus -ne "Pending") {
    Throw "Cannot approve private endpoint connection $EndpointName with status $EndpointStatus"
  }

  Write-Host "Approving private endpoint connection $EndpointName"
  $Approval = Approve-AzPrivateEndpointConnection -ResourceId $Endpoint.Id -Description $ApprovalDescription

  If ($Approval.ProvisioningState -ne "Succeeded") {
    Throw "Failed to approve private endpoint connection $EndpointName for $ResourceName"
  }

  $ApprovalStatus = $Approval.ProvisioningState
  Write-Host "Private endpoint connection approval for $EndpointName completed successfully"
  Exit 0

} Catch {
  Throw $_.Exception.Message
}
