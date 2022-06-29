# PINS Data Warehouse Infrastructure PoC
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=2.98.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "__terraform_state_resource_group_name__"
    storage_account_name = "__terraform_state_storage_name__"
    container_name       = "__terraform_state_container_name__"
    key                  = "__terraform_state_state_name__"
    use_msi              = true
  }
}

provider "azurerm" {
  features {}

  subscription_id            = "__subscription_id__"
  skip_provider_registration = true
}

# Data and Variables
data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "test" {
  name = "__resource_group_name__"
}

variable "common_tags" {
  description = "The common resource tags for the project"
  type        = map(string)
  default     = {
    costCenter   = "Cloud Practice"
    stakeholder  = "Lester March"
    deletionDate = "N/A"
  }
}

# Random
resource "random_string" "rid" {
  length  = 5
  lower   = true
  upper   = false
  special = false
}

# Resource Group
# resource "azurerm_resource_group" "dwh_poc" {
#   name     = "__resource_group_name__"
#   location = "UK South"
# }
# Use new resource group instead of existing:
# Replace data.azurerm_resource_group.test.name with azurerm_resource_group.dwh_poc.name
# Replace data.azurerm_resource_group.test.location with azurerm_resource_group.dwh_poc.location

# Network
resource "azurerm_virtual_network" "dwh_poc" {
  name                = "__virtual_network_name__"
  resource_group_name = data.azurerm_resource_group.test.name
  location            = data.azurerm_resource_group.test.location
  address_space       = ["10.0.0.0/16"]
  tags                = var.common_tags
}

resource "azurerm_subnet" "dwh_poc" {
  name                 = "__subnet_name__"
  resource_group_name  = data.azurerm_resource_group.test.name
  virtual_network_name = azurerm_virtual_network.dwh_poc.name
  address_prefixes     = ["10.0.0.0/24"]
  service_endpoints = [
    "Microsoft.Storage",
    "Microsoft.KeyVault"
  ]
}

# Key Vault
resource "azurerm_key_vault" "dwh_poc" {
  name                            = "__key_vault_name__"
  resource_group_name             = data.azurerm_resource_group.test.name
  location                        = data.azurerm_resource_group.test.location
  tenant_id                       = data.azurerm_client_config.current.tenant_id
  sku_name                        = "standard"
  soft_delete_retention_days      = 7
  purge_protection_enabled        = true
  enabled_for_template_deployment = true
  tags                            = var.common_tags

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "get",
    ]

    secret_permissions = [
      "get",
    ]

    storage_permissions = [
      "get",
    ]
  }
}

# Storage Account
resource "azurerm_storage_account" "dwh_poc" {
  name                     = "__storage_account_name__"
  resource_group_name      = data.azurerm_resource_group.test.name
  location                 = data.azurerm_resource_group.test.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  min_tls_version          = "TLS1_2"
  is_hns_enabled           = true
  tags                     = var.common_tags
}

resource "azurerm_role_assignment" "storage_blob_data_contributor_administrators" {
  scope                = azurerm_storage_account.dwh_poc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = "__aad_administrator_group_id__"
}

resource "azurerm_role_assignment" "storage_blob_data_contributor_contributors" {
  scope                = azurerm_storage_account.dwh_poc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = "__aad_contributor_group_id__"
}

resource "azurerm_role_assignment" "storage_blob_data_contributor_compute_operators" {
  scope                = azurerm_storage_account.dwh_poc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = "__aad_computeoperator_group_id__"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "dwh_poc" {
  name               = "__storage_filesystem_name__"
  storage_account_id = azurerm_storage_account.dwh_poc.id
}

# Synapse Workspace
resource "azurerm_synapse_workspace" "dwh_poc" {
  name                                 = "__synapse_workspace_name__"
  resource_group_name                  = data.azurerm_resource_group.test.name
  location                             = data.azurerm_resource_group.test.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.dwh_poc.id
  sql_administrator_login              = "sqladminuser"
  sql_administrator_login_password     = "T3st!nG123"
  managed_virtual_network_enabled      = true
  tags                                 = var.common_tags

  aad_admin {
    login     = "__aad_administrator_group_name__"
    object_id = "__aad_administrator_group_id__"
    tenant_id = data.azurerm_client_config.current.tenant_id
  }

  sql_aad_admin {
    login     = "__aad_sqladmin_group_name__"
    object_id = "__aad_sqladmin_group_id__"
    tenant_id = data.azurerm_client_config.current.tenant_id
  }

}

# Lake database containers

resource "azurerm_storage_container" "odw-curated" {
  name                  = "odw-curated"
  storage_account_name  = azurerm_storage_account.dwh_poc.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "odw-raw" {
  name                  = "odw-raw"
  storage_account_name  = azurerm_storage_account.dwh_poc.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "odw-standardised" {
  name                  = "odw-standardised"
  storage_account_name  = azurerm_storage_account.dwh_poc.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "odw-harmonised" {
  name                  = "odw-harmonised"
  storage_account_name  = azurerm_storage_account.dwh_poc.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "odw-config" {
  name                  = "odw-config"
  storage_account_name  = azurerm_storage_account.dwh_poc.name
  container_access_type = "private"
}


resource "azurerm_synapse_firewall_rule" "dwh_poc_azure" {
  name                 = "AllowAllWindowsAzureIps"
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "0.0.0.0"
}

resource "azurerm_synapse_firewall_rule" "dwh_poc_all" {
  name                 = "AllowAll"
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  start_ip_address     = "0.0.0.0"
  end_ip_address       = "255.255.255.255"
}

resource "azurerm_synapse_role_assignment" "dwh_poc_administrator" {
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  role_name            = "Synapse Administrator"
  principal_id         = "__aad_administrator_group_id__"

  depends_on = [azurerm_synapse_firewall_rule.dwh_poc_all]
}

resource "azurerm_synapse_role_assignment" "dwh_poc_contributor" {
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  role_name            = "Synapse Contributor"
  principal_id         = "__aad_contributor_group_id__"

  depends_on = [azurerm_synapse_firewall_rule.dwh_poc_all]
}

resource "azurerm_synapse_role_assignment" "dwh_poc_computeoperator" {
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  role_name            = "Synapse Compute Operator"
  principal_id         = "__aad_computeoperator_group_id__"

  depends_on = [azurerm_synapse_firewall_rule.dwh_poc_all]
}

resource "azurerm_role_assignment" "storage_blob_data_contributor_managed_identity" {
  scope                = azurerm_storage_account.dwh_poc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.dwh_poc.identity[0].principal_id
}

resource "azurerm_role_assignment" "contributor_administrator" {
  scope                = azurerm_synapse_workspace.dwh_poc.id
  role_definition_name = "Contributor"
  principal_id         = "__aad_administrator_group_id__"
}

# Synapse Spark Pool
resource "azurerm_synapse_spark_pool" "dwh_poc" {
  name                           = "__synapse_sparkpool_name__"
  synapse_workspace_id           = azurerm_synapse_workspace.dwh_poc.id
  node_size_family               = "MemoryOptimized"
  node_size                      = "Small"
  node_count                     = 3

  auto_pause {
    delay_in_minutes = 15
  }
}

# Synapse SQL Pool
resource "azurerm_synapse_sql_pool" "dwh_poc" {
  name                 = "__dedicated_sqlpool_name__"
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  collation            = "SQL_Latin1_General_CP1_CI_AS"
}

# Azure SQL Server

resource "azurerm_mssql_server" "dwh_poc" {
  name                         = "__sql_server_name__"
  resource_group_name          = data.azurerm_resource_group.sbox.name
  location                     = data.azurerm_resource_group.sbox.location
  version                      = "12.0"
  administrator_login          = "sqladmin"
  administrator_login_password = "__sql_server_pwd__"
  minimum_tls_version          = "1.2"

  azuread_administrator {
    login_username = "__aad_administrator_group_name__"
    object_id      = "__aad_administrator_group_id__"
  }
  tags                = var.common_tags
}