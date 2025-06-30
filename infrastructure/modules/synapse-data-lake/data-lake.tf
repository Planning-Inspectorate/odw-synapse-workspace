
resource "azurerm_storage_account" "synapse" {
  #checkov:skip=CKV2_AZURE_40: Ensure storage account is not configured with Shared Key authorization (checkov v3)
  #checkov:skip=CKV2_AZURE_47: Ensure storage account is configured without blob anonymous access (checkov v3)
  #checkov:skip=CKV2_AZURE_41: Ensure storage account is configured with SAS expiration policy (checkov v3)
  #checkov:skip=CKV_AZURE_244: Avoid the use of local users for Azure Storage unless necessary (checkov v3)
  #checkov:skip=CKV_AZURE_35: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_59: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_190: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_206: Storage replication is defined in environment variables with ZRS default
  #checkov:skip=CKV2_AZURE_1: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_8: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV2_AZURE_18: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_33: Private Endpoint is not enabled as networking is controlled by Firewall
  name                             = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  resource_group_name              = "GRS"
  location                         = var.location
  account_tier                     = var.data_lake_account_tier
  account_replication_type         = var.data_lake_replication_type
  account_kind                     = "StorageV2"
  default_to_oauth_authentication  = true
  https_traffic_only_enabled       = true
  is_hns_enabled                   = true
  min_tls_version                  = "TLS1_2"
  public_network_access_enabled    = true
  cross_tenant_replication_enabled = true

  blob_properties {
    delete_retention_policy {
      days = var.data_lake_retention_days
    }

    container_delete_retention_policy {
      days = var.data_lake_retention_days
    }
  }

  queue_properties {
    logging {
      read                  = true
      write                 = true
      delete                = true
      retention_policy_days = var.data_lake_retention_days
      version               = "1.0"
    }

    minute_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = var.data_lake_retention_days
      version               = "1.0"
    }

    hour_metrics {
      enabled               = true
      include_apis          = true
      retention_policy_days = var.data_lake_retention_days
      version               = "1.0"
    }
  }

  tags = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.synapse.id
}

resource "azurerm_storage_container" "synapse" {
  #checkov:skip=CKV2_AZURE_21: Implemented in synapse-monitoring module
  for_each = toset(var.data_lake_storage_containers)

  name                  = each.key
  storage_account_name  = azurerm_storage_account.synapse.name
  container_access_type = "private"

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.synapse
  ]
}
