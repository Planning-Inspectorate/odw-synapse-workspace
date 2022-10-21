variable "devops_agent_subnet_name" {
  default     = "ComputeSubnet"
  description = "The name of the subnet into which the devops agents will be deployed"
  type        = string
}

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "network_watcher_enabled" {
  default     = false
  description = "Determines whether a Network Watcher resource will be deployed"
  type        = bool
}

variable "resource_group_id" {
  description = "The ID of the resource group into which resources will be deployed"
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

variable "vnet_base_cidr_block" {
  default     = "10.90.0.0/24"
  description = "The base IPv4 range for the Virtual Network in CIDR notation"
  type        = string
}

variable "vnet_subnets" {
  default = [
    {
      "name" : "AzureBastionSubnet",
      "new_bits" : 2 # /26
    },
    {
      "name" : "SynapseEndpointSubnet",
      "new_bits" : 2 # /26
    },
    {
      "name" : "ComputeSubnet"
      "new_bits" : 2 # /26
    },
    {
      "name" : null, # Reserved
      "new_bits" : 2 # /26
    }
  ]
  description = "A collection of subnet definitions used to logically partition the Virtual Network"
  type        = list(map(string))
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}
