resource "azurerm_virtual_machine_extension" "custom_script_runtime" {
  name                 = "IntegrationRuntimeSetup"
  virtual_machine_id   = azurerm_windows_virtual_machine.synapse.id
  publisher            = "Microsoft.Compute"
  type                 = "CustomScriptExtension"
  type_handler_version = "1.9"

  protected_settings = <<PROTECTED_SETTINGS
    {
      "fileUris": ["${azurerm_storage_blob.runtime_script.url}"],
      "commandToExecute": "powershell.exe -ExecutionPolicy Unrestricted -File ${local.script_name_runtime} -authKey ${azurerm_synapse_integration_runtime_self_hosted.synapse.authorization_key_primary}",
      "managedIdentity": {}
    }
PROTECTED_SETTINGS

  depends_on = [
    azurerm_role_assignment.shir_vm,
    azurerm_synapse_integration_runtime_self_hosted.synapse
  ]

  tags = local.tags
}

resource "azurerm_virtual_machine_extension" "custom_script_openjdk" {
  name                 = "OpenJdkSetup"
  virtual_machine_id   = azurerm_windows_virtual_machine.synapse.id
  publisher            = "Microsoft.Compute"
  type                 = "CustomScriptExtension"
  type_handler_version = "1.9"

  protected_settings = <<PROTECTED_SETTINGS
    {
      "fileUris": ["${azurerm_storage_blob.openjdk_script.url}"],
      "commandToExecute": "powershell.exe -ExecutionPolicy Unrestricted -File ${local.script_name_openjdk}",
      "managedIdentity": {}
    }
PROTECTED_SETTINGS

  depends_on = [
    azurerm_role_assignment.shir_vm,
  ]

  tags = local.tags
}
