
variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "resource_group_name" {
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

variable "odt_back_office_service_bus_name" {
  description = "The name of the Service Bus namespace into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_resource_group_name" {
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

variable "synapse_private_endpoint_subnet_name" {
  default     = "SynapseEndpointSubnet"
  description = "The name of the subnet into which Synapse private endpoints should be deployed"
  type        = string
}

variable "synapse_private_endpoint_vnet_subnets" {
  description = "A map of subnet names and IDs comprising the linked Virtual Network for private endpoint deployment"
  type        = map(string)
}

# variable "synapse_workspace_failover_principal_id" {
#   default     = null
#   description = "The managed identity for the failover Synapse Workspace"
#   type        = string
# }

variable "synapse_workspace_principal_id" {
  description = "The managed identity for the Synapse Workspace"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}

variable "odt_back_office_private_endpoint_dns_zone_id" {
  description = "The ID of the private DNS zone for the ODT Back Office private dns zone"
  type        = string
}
