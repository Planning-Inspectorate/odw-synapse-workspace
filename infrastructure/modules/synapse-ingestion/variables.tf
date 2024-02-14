variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "failover_namespace" {
  description = "Determines whether the Service Bus Namespace will be configured as a failover instance"
  type        = bool
}

variable "primary_service_bus_namespace_id" {
  default     = null
  description = "The ID of the Service Bus Namespace to replicate from if failover_namespace is true"
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
      "subscription_name" { =
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

variable "service_name" {
  description = "The short-format name of the overarching service being deployed"
  type        = string
}

variable "synapse_workspace_failover_principal_id" {
  default     = null
  description = "The managed identity for the failover Synapse Workspace"
  type        = string
}

variable "synapse_workspace_principal_id" {
  description = "The managed identity for the Synapse Workspace"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}
