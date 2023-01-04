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

variable "service_bus_role_assignments" {
  default     = {}
  description = "An object mapping RBAC roles to principal IDs for the service bus"
  type        = map(list(string))
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
