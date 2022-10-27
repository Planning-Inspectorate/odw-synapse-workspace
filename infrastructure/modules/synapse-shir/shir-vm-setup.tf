resource "azurerm_virtual_machine_extension" "custom_script" {
  name                 = "shir-ext-${random_string.unique_id.id}"
  virtual_machine_id   = azurerm_windows_virtual_machine.synapse.id
  publisher            = "Microsoft.Azure.Extensions"
  type                 = "CustomScript"
  type_handler_version = "2.0"

  settings = <<SETTINGS
  {
    "fileUris": ["${azurerm_storage_blob.install_shir.url}"],
    "commandToExecute": "powershell.exe Install-Shir.ps1",
    "managedIdentity" : {}
  }
SETTINGS

  depends_on = [
    azurerm_role_assignment.shir_vm
  ]

  tags = local.tags
}
