resource "azurerm_storage_account" "shir" {
  #checkov:skip=CKV_AZURE_35: Firewall not required for this stroage account
  #checkov:skip=CKV_AZURE_59: Firewall not required for this stroage account
  #checkov:skip=CKV_AZURE_190: Firewall not required for this stroage account
  #checkov:skip=CKV_AZURE_206: Locally redundant storage is acceptable for this storage account use-case
  #checkov:skip=CKV2_AZURE_1: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_8: Firewall not required for this stroage account
  #checkov:skip=CKV2_AZURE_18: Microsoft managed keys are acceptable
  name                            = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  default_to_oauth_authentication = true
  enable_https_traffic_only       = true
  min_tls_version                 = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 7
    }

    container_delete_retention_policy {
      days = 7
    }
  }

  queue_properties {
    logging {
      read                  = true
      write                 = true
      delete                = true
      retention_policy_days = 7
      version               = "1.0"
    }

    minute_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = 7
      version               = "1.0"
    }

    hour_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = 7
      version               = "1.0"
    }
  }

  tags = local.tags
}

resource "azurerm_storage_container" "shir" {
  #checkov:skip=CKV_AZURE_34: Public access is required
  #checkov:skip=CKV2_AZURE_21: Blob logging is not required
  name                  = "scripts"
  storage_account_name  = azurerm_storage_account.shir.name
  container_access_type = "container"
}

resource "azurerm_storage_blob" "deploy_script" {
  name                   = local.script_name_deploy
  storage_account_name   = azurerm_storage_account.shir.name
  storage_container_name = azurerm_storage_container.shir.name
  type                   = "Block"
  source                 = "${path.module}/scripts/${local.script_name_deploy}"
}

resource "azurerm_storage_blob" "runtime_script" {
  name                   = local.script_name_runtime
  storage_account_name   = azurerm_storage_account.shir.name
  storage_container_name = azurerm_storage_container.shir.name
  type                   = "Block"
  source                 = "${path.module}/scripts/${local.script_name_runtime}"
}

resource "azurerm_storage_blob" "openjdk_script" {
  name                   = local.script_name_openjdk
  storage_account_name   = azurerm_storage_account.shir.name
  storage_container_name = azurerm_storage_container.shir.name
  type                   = "Block"
  source                 = "${path.module}/scripts/${local.script_name_openjdk}"
}
