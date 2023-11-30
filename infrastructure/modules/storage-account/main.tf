resource "azurerm_storage_account" "storage" {
  #checkov:skip=CKV_AZURE_33: Ensure Storage logging is enabled for Queue service for read, write and delete requests
  #checkov:skip=CKV_AZURE_35: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_59: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_190: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV_AZURE_206: Locally redundant storage is acceptable for this storage account use-case
  #checkov:skip=CKV2_AZURE_1: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_8: Firewall is enabled using azurerm_storage_account_network_rules
  #checkov:skip=CKV2_AZURE_18: Microsoft managed keys are acceptable
  #checkov:skip=CKV2_AZURE_33: Private Endpoint is not enabled as networking is controlled by Firewall
  name                      = replace("pins-st-${local.resource_suffix}-${random_string.unique_id.id}", "-", "")
  resource_group_name       = var.resource_group_name
  location                  = var.location
  account_tier              = var.storage_tier
  account_replication_type  = var.storage_replication
  account_kind              = var.account_kind
  enable_https_traffic_only = true
  min_tls_version           = "TLS1_2"
  access_tier               = var.access_tier
  is_hns_enabled            = var.is_hns_enabled
  large_file_share_enabled  = var.large_file_share_enabled

  dynamic "custom_domain" {
    for_each = var.custom_domain
    content {
      name          = custom_domain.value["name"]
      use_subdomain = custom_domain.value["use_subdomain"]
    }
  }

  identity {
    type = "SystemAssigned"
  }

  dynamic "blob_properties" {
    for_each = local.soft_delete_retention_policy == true ? toset([1]) : toset([])

    content {
      delete_retention_policy {
        days = 90
      }
      container_delete_retention_policy {
        days = 90
      }
    }
  }
  dynamic "static_website" {
    for_each = var.static_website
    content {
      index_document     = static_website.value.index_document
      error_404_document = static_website.value.error_404_document
    }
  }

  tags = local.tags
}

resource "azurerm_storage_account_network_rules" "storage_network_rule" {
  storage_account_id         = azurerm_storage_account.storage.id
  default_action             = var.network_default_action
  ip_rules                   = var.network_rule_ips
  virtual_network_subnet_ids = var.network_rule_virtual_network_subnet_ids_include_cicd_agents ? concat(local.cicd_subnet_ids, var.network_rule_virtual_network_subnet_ids) : var.network_rule_virtual_network_subnet_ids
  bypass                     = var.network_rule_bypass
}

resource "azurerm_storage_container" "container" {
  #checkov:skip=CKV2_AZURE_21: "Ensure Storage logging is enabled for Blob service for read requests"
  for_each              = toset(var.container_name)
  name                  = each.key
  storage_account_name  = azurerm_storage_account.storage.name
  container_access_type = var.container_access_type #TODO: this needs to be a list
}

resource "azurerm_storage_blob" "blob" {
  count                  = length(var.blobs)
  name                   = var.blobs[count.index].name
  storage_container_name = var.blobs[count.index].container_name
  storage_account_name   = azurerm_storage_account.storage.name
  type                   = var.blobs[count.index].type
  size                   = contains(keys(var.blobs[count.index]), "size") ? var.blobs[count.index].size : 0
  source                 = contains(keys(var.blobs[count.index]), "source") ? var.blobs[count.index].source : null
  content_md5            = contains(keys(var.blobs[count.index]), "source") && var.blobs[count.index].type == "Block" ? filemd5(var.blobs[count.index].source) : null
  depends_on             = [azurerm_storage_container.container]
}

resource "azurerm_storage_queue" "queue" {
  for_each             = toset(var.queue_name)
  name                 = each.key
  storage_account_name = azurerm_storage_account.storage.name
}

resource "azurerm_storage_share" "share" {
  count                = length(var.shares)
  name                 = var.shares[count.index].name
  storage_account_name = azurerm_storage_account.storage.name
  quota                = var.shares[count.index].quota
}

resource "azurerm_storage_share_directory" "share_directories" {
  for_each             = var.share_directories
  name                 = each.key
  share_name           = each.value
  storage_account_name = azurerm_storage_account.storage.name
  depends_on           = [azurerm_storage_share.share]
}

resource "azurerm_storage_table" "table" {
  #checkov:skip=CKV2_AZURE_20: Ensure Storage logging is enabled for Table service for read requests
  for_each             = toset(var.tables)
  name                 = each.key
  storage_account_name = azurerm_storage_account.storage.name
}

resource "azurerm_storage_data_lake_gen2_filesystem" "storage_dlg2fs" {
  for_each           = toset(var.dlg2fs)
  name               = each.key
  storage_account_id = azurerm_storage_account.storage.id
}
