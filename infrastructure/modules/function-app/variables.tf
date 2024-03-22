/*
    Terraform configuration file defining variables
*/
variable "application_insights_key" {
  type        = string
  description = "The key for the application insights instance"
  default     = null
}

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
  description = "List of tags to be applied to resources"
  type        = map(string)
  default     = {}
}

variable "resource_group_name" {
  type        = string
  description = "The target resource group this module should be deployed into. If not specified one will be created for you with name like: environment-application-template-location"
  default     = ""
}

variable "file_share_name" {
  type        = string
  description = "The name of the file share to create"
}

variable "function_app_name" {
  type        = string
  description = "Name of the function app"
}

# app service plan related

variable "service_plan_id" {
  type        = string
  description = "ID of the app service plan instance to host this app service. If unspecified one will be created for you"
  default     = null
}

variable "identity_ids" {
  type        = list(string)
  description = "List of service principal IDs if you want to use a User Assigned Identity over a System Assigned Identity"
  default     = []
}

variable "service_name" {
  type        = string
  description = "Name of the service"
}

variable "message_storage_account" {
  type        = string
  description = "Name of the storage account for service bus messages"
  default     = null
}

variable "message_storage_container" {
  type        = string
  description = "Name of the storage account container for service bus messages"
  default     = null
}

variable "storage_account_name" {
  type        = any
  description = "The name of the backend storage account"
  default     = null
}

variable "storage_account_access_key" {
  type        = any
  description = "The key to access the backend storage account"
  default     = null
  sensitive   = true
}

variable "functions_extension_version" {
  type        = string
  description = "The version of the Azure Functions runtime to use"
  default     = "~4"
}

variable "app_settings" {
  type        = map(string)
  description = "Function app settings"
  default     = {}
}

variable "servicebus_namespace" {
  type        = string
  description = "The name of the service bus namespace to use for the function app"
  default     = null
}

variable "site_config_defaults" {
  type = object({
    always_on = bool
    cors = object({
      allowed_origins     = list(string)
      support_credentials = bool
    })
    ftps_state                  = string
    health_check_path           = string
    http2_enabled               = bool
    linux_fx_version            = string
    minimum_tls_version         = string
    pre_warmed_instance_count   = number
    scm_use_main_ip_restriction = bool
    use_32_bit_worker           = bool
    websockets_enabled          = bool
    vnet_route_all_enabled      = bool
    application_stack = object({
      dotnet_version          = string
      use_dotnet_isolated     = bool
      java_version            = string
      python_version          = string
      powershell_core_version = string
      use_custom_runtime      = bool
    })
    ip_restrictions = object({
      ip_addresses = list(object({
        rule_name  = string
        ip_address = string
        priority   = number
        action     = string
      }))
      service_tags = list(object({
        rule_name        = string
        service_tag_name = string
        priority         = number
        action           = string
      }))
      subnet_ids = list(object({
        rule_name = string
        subnet_id = string
        priority  = number
        action    = string
      }))
    })
  })
  description = "A site config block for configuring the function"
  default = {
    always_on = false
    cors = {
      allowed_origins     = ["*", "https://portal.azure.com"]
      support_credentials = false
    }
    ftps_state                  = "Disabled"
    health_check_path           = null
    http2_enabled               = true
    linux_fx_version            = null
    minimum_tls_version         = 1.2
    pre_warmed_instance_count   = null
    scm_use_main_ip_restriction = true
    use_32_bit_worker           = false
    websockets_enabled          = false
    vnet_route_all_enabled      = false
    application_stack = {
      dotnet_version          = ""
      use_dotnet_isolated     = false
      java_version            = ""
      python_version          = ""
      powershell_core_version = ""
      use_custom_runtime      = false
    }
    ip_restrictions = {
      ip_addresses = []
      service_tags = []
      subnet_ids   = []
    }
  }
}

variable "site_config" {
  type        = any
  description = "Site config to override site_config_defaults. Object structure identical to site_config_defaults"
  default     = {}
}


variable "synapse_function_app_subnet_name" {
  default     = "FunctionAppSubnet"
  description = "The name of the subnet into which the function App's should be deployed"
  type        = string
}

# variable "synapse_vnet_security_groups" {
#   description = "A map of subnet names to network security group IDs"
#   type        = map(string)
# }

variable "synapse_vnet_subnet_names" {
  description = "A map of subnet names to IDs comprising the linked Virtual Network for Function App deployment"
  type        = map(string)
}
