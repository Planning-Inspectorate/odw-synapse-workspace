data "azurerm_client_config" "current" {}

data "azurerm_storage_account" "synapse" {
  name                = var.data_lake_account_name
  resource_group_name = var.resource_group_name
}
