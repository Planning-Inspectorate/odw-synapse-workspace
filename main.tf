# PINS Data Warehouse Infrastructure PoC
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "=2.46.0"
    }
  }
}

provider "azurerm" {
  features {}

  subscription_id            = "1a4067df-ed97-4ff7-b07f-4cd13f92c8d5"
  skip_provider_registration = true
}

# Data and Variables
data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "test" {
  name = "RG-Lester-March"
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
#   name     = "pins-rg-odw-data-uks-dev"
#   location = "UK South"
# }
# Use new resource group instead of existing:
# Replace data.azurerm_resource_group.test.name with azurerm_resource_group.dwh_poc.name
# Replace data.azurerm_resource_group.test.location with azurerm_resource_group.dwh_poc.location

# Network
resource "azurerm_virtual_network" "dwh_poc" {
  name                = "pins-vnet-odw-data-uks-dev"
  resource_group_name = data.azurerm_resource_group.test.name
  location            = data.azurerm_resource_group.test.location
  address_space       = ["10.0.0.0/16"]
  tags                = var.common_tags
}

resource "azurerm_subnet" "dwh_poc" {
  name                 = "pins-vnet-odw-data-uks-dev"
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
  name                            = "pinskvodwdatauks${random_string.rid.id}"
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
  name                     = "pinsodwdatauksdev01"
  resource_group_name      = data.azurerm_resource_group.test.name
  location                 = data.azurerm_resource_group.test.location
  account_tier             = "Standard"
  account_replication_type = "ZRS"
  min_tls_version          = "TLS1_2"
  is_hns_enabled           = true
  tags                     = var.common_tags
}

resource "azurerm_role_assignment" "storage_blob_data_contributor" {
  scope                = data.azurerm_resource_group.test.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "dwh_poc" {
  name               = "odw-workspace"
  storage_account_id = azurerm_storage_account.dwh_poc.id
}

# Synapse Workspace
resource "azurerm_synapse_workspace" "dwh_poc" {
  name                                 = "pinssynws01"
  resource_group_name                  = data.azurerm_resource_group.test.name
  location                             = data.azurerm_resource_group.test.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.dwh_poc.id
  sql_administrator_login              = "sqladminuser"
  sql_administrator_login_password     = "T3st!nG123"
  tags                                 = var.common_tags

  aad_admin {
    login     = "pins-odw-data-dev-syn-ws-sqladmins"
    object_id = "1c996957-30e4-40fe-b0b4-82d40f13c058"
    tenant_id = data.azurerm_client_config.current.tenant_id
  }
}

# Synapse Spark Pool
resource "azurerm_synapse_spark_pool" "dwh_poc" {
  name                           = "sparkpool01"
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
  name                 = "sqlpool01"
  synapse_workspace_id = azurerm_synapse_workspace.dwh_poc.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  collation            = "SQL_Latin1_General_CP1_CI_AS"
}