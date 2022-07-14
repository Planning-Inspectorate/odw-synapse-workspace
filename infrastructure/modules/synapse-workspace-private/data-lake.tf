resource "azurerm_storage_account" "synapse" {
  #checkov:skip=CKV_AZURE_35:   TODO: Set default network access to deny
  #checkov:skip=CKV2_AZURE_1:   TODO: Implement customer-managed keys for encryption
  #checkov:skip=CKV2_AZURE_8:   TODO: Set activity log container to private
  #checkov:skip=CKV2_AZURE_18:  TODO: Implement customer-managed keys for encryption
  name                      = replace("pins-st-${local.resource_suffix}", "-", "")
  resource_group_name       = var.resource_group_name
  location                  = var.location
  account_tier              = var.data_lake_account_tier
  account_replication_type  = var.data_lake_replication_type
  account_kind              = "StorageV2"
  enable_https_traffic_only = true
  is_hns_enabled            = true
  min_tls_version           = "TLS1_2"

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

resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.synapse.id
}

resource "azurerm_storage_container" "synapse" {
  #checkov:skip=CKV2_AZURE_21:  TODO: Set storage logging for blob service
  for_each = toset(var.data_lake_storage_containers)

  name                  = each.key
  storage_account_name  = azurerm_storage_account.synapse.name
  container_access_type = "private"
}
