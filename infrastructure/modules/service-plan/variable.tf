/*
    Terraform configuration file defining variables
*/
variable "environment" {
  type        = string
  description = "The environment name. Used as a tag and in naming the resource group"
}

variable "location" {
  type        = string
  description = "The region resources will be deployed to"
  default     = "uksouth"
}

variable "tags" {
  type        = map(string)
  description = "List of tags to be applied to resources"
  default     = {}
}

variable "resource_group_name" {
  type        = string
  description = "The target resource group this module should be deployed into. If not specified one will be created for you with name like: environment-application-template-location"
}

variable "service_name" {
  type        = string
  description = "Name of the service"
}

variable "os_type" {
  type        = string
  description = "Kind of app service plan, Wndows, Linux or elastic"
  default     = "Linux"
}

variable "worker_count" {
  type        = string
  description = "Maximum number of workers for an Elastic sclaed App Service Plan"
  default     = null
}

variable "maximum_elastic_worker_count" {
  type        = string
  description = "Maximum number of workers for an Elastic sclaed App Service Plan"
  default     = null
}

variable "sku_name" {
  type        = string
  description = "The Sku of the ASP"
  default     = "EP1"
}
variable "per_site_scaling_enabled" {
  type        = bool
  description = "Can apps independently scale with this ASP?"
  default     = false
}

variable "app_service_env_id" {
  type        = string
  description = "ASE where the ASP should be located"
  default     = null
}

variable "zone_balancing_enabled" {
  type        = bool
  description = "Is zone balancing enabled?"
  default     = false
}
