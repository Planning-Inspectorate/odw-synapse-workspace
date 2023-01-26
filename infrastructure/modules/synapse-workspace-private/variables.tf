variable "data_lake_account_id" {
  description = "The ID of the Data Lake Storage Account"
  type        = string
}

variable "data_lake_account_id_failover" {
  description = "The ID of the Data Lake Storage Account used for backup and failover"
  type        = string
}

variable "data_lake_account_name" {
  description = "The name of the Data Lake Storage Account"
  type        = string
}

variable "data_lake_account_name_failover" {
  description = "The name of the Data Lake Storage Account used for backup and failover"
  type        = string
}

variable "data_lake_filesystem_id" {
  description = "The ID of the Data Lake Gen2 filesystem"
  type        = string
}

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "firewall_allowed_ip_addresses" {
  default     = []
  description = "A list of CIDR ranges to be permitted access to the data lake Storage Account"
  type        = list(string)
}

variable "key_vault_id" {
  description = "The ID of the Key Vault to use for secret storage"
  type        = string
}

variable "key_vault_name" {
  description = "The name of the Key Vault to use for secret storage"
  type        = string
}

variable "network_resource_group_name" {
  description = "The name of the resource group into which private endpoints will be deployed"
  type        = string
}

variable "purview_id" {
  default     = null
  description = "The ID of the Purview account to link with the Synapse Workspace"
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

variable "spark_pool_enabled" {
  default     = false
  description = "Determines whether a Synapse-linked Spark pool should be deployed"
  type        = bool
}

variable "spark_pool_max_node_count" {
  default     = 9
  description = "The maximum number of nodes the Synapse-linked Spark pool can autoscale to"
  type        = number
}

variable "spark_pool_min_node_count" {
  default     = 3
  description = "The minimum number of nodes the Synapse-linked Spark pool can autoscale to"
  type        = number
}

variable "spark_pool_node_size" {
  default     = "Small"
  description = "The size of nodes comprising the Synapse-linked Spark pool"
  type        = string
}

variable "spark_pool_preview_enabled" {
  default     = false
  description = "Determines whether a Synapse-linked preview Spark pool should be deployed"
  type        = bool
}

variable "spark_pool_preview_version" {
  default     = "3.3"
  description = "The version of Spark running on the Synapse-linked preview Spark pool"
  type        = string
}

variable "spark_pool_requirements" {
  default     = null
  description = "File contents containing a list of packages required by the Spark pool"
  type        = string
}

variable "spark_pool_timeout_minutes" {
  default     = 15
  description = "The time buffer in minutes to wait before the Spark pool is paused due to inactivity"
  type        = number
}

variable "spark_pool_version" {
  default     = "3.2"
  description = "The version of Spark running on the Synapse-linked Spark pool"
  type        = string
}

variable "sql_pool_collation" {
  default     = "SQL_Latin1_General_CP1_CI_AS"
  description = "The collation of the Synapse-linked dedicated SQL pool"
  type        = string
}

variable "sql_pool_enabled" {
  default     = false
  description = "Determines whether a Synapse-linked dedicated SQL pool should be deployed"
  type        = bool
}

variable "sql_pool_sku_name" {
  default     = "DW100c"
  description = "The SKU of the Synapse-linked dedicated SQL pool"
  type        = string
}

variable "synapse_aad_administrator" {
  description = "A map describing the username and Azure AD object ID for the Syanapse administrator account"
  type        = map(string)
}

variable "synapse_data_exfiltration_enabled" {
  default     = false
  description = "Determines whether the Synapse Workspace should have data exfiltration protection enabled"
  type        = bool
}

variable "synapse_private_endpoint_dns_zone_id" {
  description = "The ID of the Private DNS Zone hosting privatelink.azuresynapse.net"
  type        = string
}

variable "synapse_private_endpoint_subnet_name" {
  default     = "SynapseEndpointSubnet"
  description = "The name of the subnet into which Synapse private endpoints should be deployed"
  type        = string
}

variable "synapse_private_endpoint_vnet_subnets" {
  description = "A map of subnet names and IDs comprising the linked Virtual Network for private endpoint deployment"
  type        = map(string)
}

variable "synapse_role_assignments" {
  default     = {}
  description = "An object mapping RBAC roles to principal IDs for the Synapse Workspace"
  type        = map(list(string))
}

variable "synapse_sql_administrator_username" {
  default     = "synadmin"
  description = "The SQL administrator username for the Synapse Workspace"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}

variable "tenant_id" {
  description = "The ID of the Azure AD tenant containing the identities used for RBAC assignments"
  type        = string
}
