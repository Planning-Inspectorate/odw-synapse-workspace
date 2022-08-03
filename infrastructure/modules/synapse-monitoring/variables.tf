variable "environment" {
  description = "The name of the environment in which resources will be deployed"
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

variable "log_retention_days" {
  default     = 30
  description = "The number of days to retain logs in the Log Analytics Workspace"
  type        = number
}

variable "service_name" {
  description = "The short-format name of the overarching service being deployed"
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
