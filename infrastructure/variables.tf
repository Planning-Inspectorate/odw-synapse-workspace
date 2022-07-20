variable "data_lake_account_tier" {
  default     = "Standard"
  description = "The tier of the Synapse data lake Storage Account"
  type        = string
}

variable "data_lake_replication_type" {
  default     = "ZRS"
  description = "The replication type for the Synapse data lake Storage Account"
  type        = string
}

variable "data_lake_role_assignments" {
  default     = {}
  description = "The RBAC assignments to be applied to the Synapse data lake Storage Account"
  type        = map(string)
}

variable "data_lake_storage_containers" {
  default     = ["default"]
  description = "A list of container names to be created in the Synapse data lake Storage Account"
  type        = list(string)
}

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "key_vault_role_assignments" {
  default     = {}
  description = "The RBAC assignments to be applied to the Key Vault"
  type        = map(string)
}

variable "location" {
  description = "The short-format Azure region into which resources will be deployed"
  type        = string
}

variable "network_watcher_enabled" {
  default     = false
  description = "Determines whether a Network Watcher resource will be deployed"
  type        = bool
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

variable "spark_pool_version" {
  default     = "2.4"
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

variable "synapse_github_details" {
  default     = {}
  description = "The GitHub repository details to establish a link with the Synapse Workspace"
  type        = map(string)
}

variable "synapse_github_enabled" {
  default     = false
  description = "Determines whether a GitHub repository should be linked to the Synapse Workspace"
  type        = bool
}

variable "synapse_role_assignments" {
  default     = {}
  description = "The Synapse-specific RBAC assignments to be applied to the Synapse Workspace"
  type        = map(string)
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

variable "vnet_base_cidr_block" {
  default     = "10.90.0.0/24"
  description = "The base IPv4 range for the Virtual Network in CIDR notation"
  type        = string
}

variable "vnet_subnets" {
  default = [
    {
      name     = "ManagementSubnet"
      new_bits = 2
    },
    {
      name     = "SynapseEndpointSubnet"
      new_bits = 2
    },
    {
      name     = null
      new_bits = 2
    },
    {
      name     = null
      new_bits = 2
    }
  ]
  description = "A collection of subnet definitions used to logically partition the Virtual Network"
  type        = list(map(string))
}
