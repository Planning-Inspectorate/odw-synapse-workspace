
variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "resource_group_name" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_failover_enabled" {
  default     = false
  description = "Whether or not to enable failover for the Service Bus namespace"
  type        = bool
}

variable "odt_back_office_service_bus_resource_group_name" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_resource_group_name_failover" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "odt_back_office_service_bus_subscription_id" {
  description = "The ID of the subscription in which resources will be deployed"
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

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}

variable "odt_back_office_private_endpoint_dns_zone_id" {
  description = "The ID of the private DNS zone for the ODT Back Office private dns zone"
  type        = string
}