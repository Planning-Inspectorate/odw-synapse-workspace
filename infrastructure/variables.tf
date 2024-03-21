variable "alert_group_platform_enabled" {
  default     = false
  description = "Determines whether the alert group for platform alerts is enabled"
  type        = bool
}

variable "alert_group_synapse_enabled" {
  default     = false
  description = "Determines whether the alert group for Synapse alerts is enabled"
  type        = bool
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
  default     = true
  description = "A switch to determine whether the devops agent pool should be deployed"
  type        = bool
}

variable "devops_agent_image_prefix" {
  default     = "devops-agents"
  description = "The name prefix used to identify the devops agent image"
  type        = string
}

variable "devops_agent_instances" {
  default     = 2
  description = "The base number of devops agents in the VM Scale Set"
  type        = number
}

variable "devops_agent_pool_resource_group_name" {
  description = "The name of the resource group into which the Azure DevOps agents VMs will be deployed"
  type        = string
}

variable "devops_agent_pool_resource_group_name_failover" {
  description = "The name of the failover resource group into which the Azure DevOps agents VMs will be deployed"
  type        = string
}

variable "devops_agent_vm_sku" {
  default     = "Standard_F2s_v2"
  description = "The size of the devops agent VMs to be deployed"
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

variable "function_app_enabled" {
  default     = false
  description = "Determines whether the resources for the Function App should be deployed"
  type        = bool
}

variable "function_app" {
  description = "A map containing the configuration for the Function App to be created"
  type        = any
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

variable "logic_app_enabled" {
  default     = false
  description = "Determines whether the resources for the App Service Plan, Storage Account and Logic App Standard should be deployed"
  type        = bool
}

variable "network_watcher_enabled" {
  default     = false
  description = "Determines whether a Network Watcher resource will be deployed"
  type        = bool
}

variable "odt_back_office_service_bus_enabled" {
  default     = false
  description = "Determines whether the ODT Service Bus Namespace will be deployed"
  type        = bool
}

variable "odt_back_office_service_bus_failover_enabled" {
  default     = false
  description = "Whether or not to enable failover for the Service Bus namespace"
  type        = bool
}


variable "odt_back_office_service_bus_name" {
  description = "The name of the Service Bus namespace into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_name_failover" {
  description = "The name of the Service Bus namespace into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_resource_group_name" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_resource_group_name_failover" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "odt_backoffice_sb_topic_subscriptions" {
  default     = {}
  type        = any
  description = <<-EOT
    "A map containing the configuration for Service Bus Subscriptions to be created in the ODT Service Bus Namespace.
    {
    subscription_name                         = "subscription_name"
    topic_name                                = "topic_name"
    status                                    = "Active"
    max_delivery_count                        = 1
    auto_delete_on_idle                       = "PT5M"
    default_message_ttl                       = "P14D"
    lock_duration                             = "P0DT0H1M0S"
    dead_lettering_on_message_expiration      = false
    dead_lettering_on_filter_evaluation_error = true
    enable_batched_operations                 = false
    requires_session                          = false
    forward_to                                = ""
    role_assignments                          = {
      "role_name" = {
        users = []
        groups = []
        service_principals = []
      }
    }"
  EOT
}

variable "odt_subscription_id" {
  description = "The subscription ID of the ODT subscription"
  type        = string
}

variable "apim_enabled" {
  default     = false
  description = "Determines whether the API Management instance should be deployed"
  type        = bool
}

variable "api_management_failover_enabled" {
  default     = false
  description = "Determines whether the API Management instance should be deployed in a failover region"
  type        = bool
}
variable "apim_publisher_email" {
  description = "The email address of the publisher of the API Management instance"
  type        = string
}

variable "apim_publisher_name" {
  description = "The name of the publisher of the API Management instance"
  type        = string
}

variable "apim_sku_name" {
  description = "The SKU name of the API Management instance"
  type        = string
}

variable "service_bus_failover_enabled" {
  default     = false
  description = "Determines whether the Service Bus Namespace will be provisioned with the Premium SKU for failover"
  type        = bool
}

variable "service_bus_role_assignments" {
  default     = {}
  type        = any
  description = <<-EOT
    "A map of maps containing the role assignments to be created in the Service Bus Namespace.
    {
      "role_assignment_name" = {
        users = [
          "user_principal_name"
        ]
        groups = [
          "group_principal_name"
        ]
        service_principals = [
          "service_principal_name"
        ]
      }
    }
  EOT
}

variable "service_bus_topics_and_subscriptions" {
  default     = {}
  type        = any
  description = <<-EOT
    "A map of maps containing the configuration for Service Bus Topics and Subscriptions to be created in the Service Bus Namespace.
    {
    name                                    = "topic_name"
    status                                  = "Active"
    auto_delete_on_idle                     = "P10675199DT2H48M5.4775807S"
    default_message_ttl                     = "P14D"
    duplicate_detection_history_time_window = "P7D"
    enable_batched_operations               = true
    enable_express                          = false
    enable_partitioning                     = false
    max_size_in_megabytes                   = 1024
    requires_duplicate_detection            = true
    support_ordering                        = false
    subscriptions                           = {
      "sunscription_name" { =
        status                                    = "Active"
        max_delivery_count                        = 1
        auto_delete_on_idle                       = "PT5M"
        default_message_ttl                       = "P14D"
        lock_duration                             = "P0DT0H1M0S"
        dead_lettering_on_message_expiration      = false
        dead_lettering_on_filter_evaluation_error = true
        enable_batched_operations                 = false
        requires_session                          = false
        forward_to                                = ""
        role_assignments                          = {
          "role_name" = {
            users = []
            groups = []
            service_principals = []
          }
        }
      }
    }
  }"
  EOT
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

variable "spark_pool_timeout_minutes" {
  default     = 15
  description = "The time buffer in minutes to wait before the Spark pool is paused due to inactivity"
  type        = number
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
  description = "A collection of subnet definitions used to logically partition the Virtual Network"
  type = list(object({
    name              = string
    new_bits          = number
    service_endpoints = list(string)
    service_delegation = list(object({
      delegation_name = string
      actions         = list(string)
    }))
  }))
}
