resource "azurerm_virtual_machine_extension" "custom_script" {
  name                 = "shir-ext-${random_string.unique_id.id}"
  virtual_machine_id   = azurerm_windows_virtual_machine.synapse.id
  publisher            = "Microsoft.Azure.Extensions"
  type                 = "CustomScript"
  type_handler_version = "2.0"

  settings = <<SETTINGS
  {
    "commandToExecute": "powershell.exe -ExecutionPolicy Unrestricted -Command $is=[IO.MemoryStream]::New([System.Convert]::FromBase64String(\\\"${base64gzip(file("${path.module}/scripts/Install-Shir.ps1"))}\\\")); $gs=[IO.Compression.GzipStream]::New($is, [IO.Compression.CompressionMode]::Decompress); $r=[IO.StreamReader]::New($gs, [System.Text.Encoding]::UTF8); Set-Content \\\"C:\\Windows\\Temp\\Install-Shir.ps1\\\" $r.ReadToEnd(); $r.Close(); .\\\"C:\\Windows\\Temp\\Install-Shir.ps1\\\" -authKey \\\"${azurerm_synapse_integration_runtime_self_hosted.synapse.authorization_key_primary}\\\""
  }
SETTINGS

  tags = local.tags
}
