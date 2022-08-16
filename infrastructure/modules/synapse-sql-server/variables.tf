variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "key_vault_id" {
  description = "The ID of the Key Vault to use for secret storage"
  type        = string
}

variable "resource_group_name" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "location" {
  description = "The short-format Azure region into which resources will be deployed"
  type        = string
}

variable "service_name" {
  description = "The short-format name of the overarching service being deployed"
  type        = string
}

variable "sql_server_aad_administrator" {
  description = "A map describing the username and Azure AD object ID for the SQL administrator account"
  type        = map(string)
}

variable "sql_server_administrator_username" {
  default     = "sqladmin"
  description = "The SQL administrator username for the SQL Server"
  type        = string
}

variable "synapse_workspace_id" {
  description = "The ID of the Synapse Workspace from which to collect diagnostic logs"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}
