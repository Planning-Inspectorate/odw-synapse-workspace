variable "alert_group_platform_enabled" {
  default     = false
  description = "Determines whether the alert group for platform alerts is enabled"
  type        = bool
}

variable "alert_group_platform_recipients" {
  default     = []
  description = "A list of email recipients to recieve platform alerts"
  type        = list(string)
}

variable "alert_group_synapse_enabled" {
  default     = false
  description = "Determines whether the alert group for Synapse alerts is enabled"
  type        = bool
}

variable "alert_group_synapse_recipients" {
  default     = []
  description = "A list of email recipients to recieve Synapse alerts"
  type        = list(string)
}

variable "alert_scope_service_health" {
  description = "The resource scope at which to alert on service health events"
  type        = string
}

variable "alert_threshold_data_lake_capacity_bytes" {
  default     = 1099511627776 # 1TiB
  description = "The threshold at which to trigger an alert for exceeding Data Lake capacity in bytes"
  type        = number
}

variable "bastion_host_enabled" {
  default     = false
  description = "Determines if a Bastion Host should be provisioned for management purposes"
  type        = bool
}

variable "bastion_vm_image" {
  default = {
    publisher = "MicrosoftWindowsDesktop"
    offer     = "windows-11"
    sku       = "win11-21h2-ent"
    version   = "latest"
  }
  description = "An object describing the image specification to use for the Bastion jumpbox VM"
  type        = map(string)
}

variable "bastion_vm_username" {
  default     = "basadmin"
  description = "The Windows administrator username for the Bastion jumpbox VM"
  type        = string
}

variable "bastion_vm_size" {
  default     = "Standard_F2s_v2"
  description = "The size of the Bastion jumpbox VM to be deployed"
  type        = string
}

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

variable "data_lake_retention_days" {
  default     = 7
  description = "The number of days blob and queue data will be retained for upon deletion"
  type        = number
}

variable "data_lake_role_assignments" {
  default     = {}
  description = "An object mapping RBAC roles to principal IDs for the data lake Storage Account"
  type        = map(list(string))
}

variable "data_lake_storage_containers" {
  default     = ["default"]
  description = "A list of container names to be created in the Synapse data lake Storage Account"
  type        = list(string)
}

variable "deploy_agent_pool" {
  default     = false
  description = "A switch to determine whether the devops agent pool should be deployed"
  type        = bool
}

variable "devops_agent_image_id" {
  description = "The ID of the VM Image to use for the devops agent VMs"
  type        = string
}

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "failover_deployment" {
  default     = false
  description = "Determines if this is a failover deployment such that resources will deployed to the failover region"
  type        = bool
}

variable "key_vault_role_assignments" {
  default     = {}
  description = "An object mapping RBAC roles to principal IDs for Key Vault"
  type        = map(list(string))
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

variable "sql_server_administrator_username" {
  default     = "sqladmin"
  description = "The SQL administrator username for the SQL Server"
  type        = string
}

variable "sql_server_enabled" {
  default     = false
  description = "Determins whether a SQL Server should be deployed"
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

variable "vnet_base_cidr_block" {
  default     = "10.90.0.0/24"
  description = "The base IPv4 range for the Virtual Network in CIDR notation"
  type        = string
}

variable "vnet_base_cidr_block_failover" {
  default     = "10.90.1.0/24"
  description = "The base IPv4 range for the failover Virtual Network in CIDR notation"
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
