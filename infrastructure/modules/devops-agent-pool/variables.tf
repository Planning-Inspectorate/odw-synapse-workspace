variable "deploy_agent_pool" {
  default     = false
  description = "A switch to determine whether the devops agent VM Scale Set should be deployed"
  type        = bool
}

variable "devops_agent_image_id" {
  default     = null
  description = "The ID of the VM Image to use for the devops agent VMs"
  type        = string
}

variable "devops_agent_instances" {
  default     = 1
  description = "The base number of devops agents in the VM Scale Set"
  type        = number
}

variable "devops_agent_username" {
  default     = "agent_user"
  description = "The username of the devops agent local account"
  type        = string
}

variable "devops_agent_subnet_name" {
  description = "The name of the subnet into which the devops agent VM Scale Set will be deployed"
  type        = string
}

variable "devops_agent_vm_sku" {
  default     = "Standard_D2ds_v5"
  description = "The size of the devops agent VMs to be deployed"
  type        = string
}

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "location" {
  description = "The short-format Azure region into which resources will be deployed"
  type        = string
}

variable "resource_group_name" {
  description = "The name of the resource group into which resources will be deployed"
  type        = string
}

variable "service_name" {
  description = "The short-format name of the overarching service being deployed"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}

variable "vnet_subnet_ids" {
  description = "A map of subnet names and IDs comprising the linked Virtual Network"
  type        = map(string)
}
