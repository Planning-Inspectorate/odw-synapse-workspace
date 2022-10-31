variable "devops_agent_subnet_name" {
  description = "The name of the subnet into which the devops agent VM Scale Set will be deployed"
  type        = string
}

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

variable "runtime_vm_image" {
  default = {
    publisher = "MicrosoftWindowsServer"
    offer     = "WindowsServer"
    sku       = "2022-datacenter-azure-edition-core"
    version   = "latest"
  }
  description = "An object describing the image specification to use for the self-hosted integration runtime VM"
  type        = map(string)
}

variable "runtime_vm_username" {
  default     = "shiradmin"
  description = "The Windows administrator username for the self-hosted integration runtime VM"
  type        = string
}

variable "runtime_vm_size" {
  default     = "Standard_F2s_v2"
  description = "The size of the self-hosted integration runtime VM to be deployed"
  type        = string
}

variable "service_name" {
  description = "The short-format name of the overarching service being deployed"
  type        = string
}

variable "synapse_workspace_id" {
  description = "The ID of the Synapse Workspace to register the self-hosted integration runtime with"
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
