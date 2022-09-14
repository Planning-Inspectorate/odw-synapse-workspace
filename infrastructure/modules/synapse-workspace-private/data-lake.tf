
resource "azurerm_storage_account" "synapse" {
  #checkov:skip=CKV_AZURE_35:   TODO: Set default network access to deny
  #checkov:skip=CKV2_AZURE_1:   SKIP: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_8:   TODO: Set activity log container to private
  #checkov:skip=CKV2_AZURE_18:  SKIP: Microsoft managed keys are acceptable
  name                            = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  resource_group_name             = var.resource_group_name
  location                        = var.location
  account_tier                    = var.data_lake_account_tier
  account_replication_type        = var.data_lake_replication_type
  account_kind                    = "StorageV2"
  default_to_oauth_authentication = true
  enable_https_traffic_only       = true
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  public_network_access_enabled   = true

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
  #checkov:skip=CKV2_AZURE_21:  SKIP: Implemented in synapse-monitoring module
  for_each = toset(var.data_lake_storage_containers)

  name                  = each.key
  storage_account_name  = azurerm_storage_account.synapse.name
  container_access_type = "private"
}
