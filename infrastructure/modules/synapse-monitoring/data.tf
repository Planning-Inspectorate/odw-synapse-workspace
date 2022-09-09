data "azurerm_key_vault_secret" "data_lake_storage_account_key" {
  name         = "data-lake-storage-account-key"
  key_vault_id = var.key_vault_id
}

data "azurerm_subscription" "current" {}
