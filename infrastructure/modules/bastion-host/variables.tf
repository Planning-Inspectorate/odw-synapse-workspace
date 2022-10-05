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

variable "environment" {
  description = "The name of the environment in which resources will be deployed"
  type        = string
}

variable "key_vault_id" {
  description = "The ID of the Key Vault to use for secret storage"
  type        = string
}

variable "network_resource_group_name" {
  description = "The name of the resource group into which private endpoints will be deployed"
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

variable "synapse_vnet_subnets" {
  description = "A map of subnet names and IDs comprising the linked Virtual Network for Bastion host deployment"
  type        = map(string)
}

variable "synapse_compute_subnet_name" {
  default     = "ComputeSubnet"
  description = "The name of the subnet into which the Bastion jumpbox VM should be deployed"
  type        = string
}

variable "tags" {
  default     = {}
  description = "A collection of tags to assign to taggable resources"
  type        = map(string)
}
