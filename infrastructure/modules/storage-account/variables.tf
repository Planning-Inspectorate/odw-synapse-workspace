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

variable "storage_tier" {
  type        = string
  description = "Quality of storage to use. Accepts Standard or Premium"
  default     = "Standard"
}

variable "storage_replication" {
  type        = string
  description = "Type of replication. Accepts LRS, GRS, RAGRS and ZRS"
  default     = "LRS"
}

variable "account_kind" {
  type        = string
  description = "Valid options are BlobStorage, BlockBlobStorage, FileStorage, Storage and StorageV2"
  default     = "StorageV2"
}

variable "access_tier" {
  type        = string
  description = "Performance/Access tier. Accepts Hot, Cool"
  default     = "Hot"
}

variable "is_hns_enabled" {
  type        = bool
  description = "Is Hierarchical Namespace enabled?"
  default     = false
}

variable "custom_domain" {
  type        = list(map(string))
  description = "A custom domain that should be used for the storage account"
  default     = []
}

variable "static_website" {
  type        = map(string)
  description = <<-EOT
  "A static website that should be hosting from this storage account. The amp format must be:
  static_website = {
    index_document     = "your_index.html"
    error_404_document = "a_page_to_display_for_404"
  }"
  EOT
  default     = {}
}

variable "network_default_action" {
  type        = string
  description = "If a source IPs fails to match a rule should it be allowed for denied"
  default     = "Deny"
}

variable "network_rules_enabled" {
  type        = bool
  description = "Is network rules enabled for this storage account?"
  default     = true
}

variable "network_rule_ips" {
  type        = list(string)
  description = "List of public IPs that are allowed to access the storage account. Private IPs in RFC1918 are not allowed here"
  default     = []
}

variable "network_rule_virtual_network_subnet_ids" {
  type        = list(string)
  description = "List of subnet IDs which are allowed to access the storage account"
  default     = []
}

variable "network_rule_bypass" {
  type        = list(string)
  description = "Specifies whether traffic is bypassed for Logging/Metrics/AzureServices. Valid options are any combination of Logging, Metrics, AzureServices, or None"
  default     = ["AzureServices", "Logging", "Metrics"]
}

variable "container_name" {
  type        = list(string)
  description = "List of names for created containers"
  default     = []
}

variable "container_access_type" {
  type        = string
  description = "The Access Level configured for this Container. Possible values are blob, container or private"
  default     = "private"
}

variable "blobs" {
  type        = list(map(string))
  description = <<-EOT
  "List of maps detailing blobs with the following structure (size must be 0 or in multiples of 512):
  [
    {
      name           = "exampleblob"
      container_name = "first-container"
      type           = "Block"
      size           = "512" #optional
      source         = "./dist/somezip.zip" #optional
    }
  ]"
  EOT
  default     = []
}

variable "queue_name" {
  type        = list(string)
  description = "List of names for queues"
  default     = []
}

variable "shares" {
  type        = list(map(string))
  description = <<-EOT
  "List of maps detailing shares with the following structure:
  [
    {
      name  = "exampleshare"
      quota = "101"
    }
  ]"
  EOT
  default     = []
}

variable "share_directories" {
  type        = map(string)
  description = <<-EOT
  "Map of share directory names where the value is the name of the share. For example:
  dirOne = "exampleshare"
  dirTwo = "exampleshare""
  EOT
  default     = {}
}

variable "tables" {
  type = list(string)
  # type = list(object({
  #   name          = string
  #   partition_key = string
  #   row_key       = string
  #   entity        = map(string)
  # }))
  description = "List of table names"
  default     = []
}

variable "dlg2fs" {
  type        = list(string)
  description = "List of data lake gen2 filesystem names (only lowercase alphanumeric characters and hyphens allowed)"
  default     = []
}

variable "large_file_share_enabled" {
  description = "Bool to define whether Large File Share is enabled"
  default     = "false"
  type        = bool
}

variable "directory_type" {
  type        = string
  description = "Defines whether the Storage Account will be joined to the AD in order to provide identity based auth / RBAC to File Shares"
  default     = "not-ad-joined"

  validation {
    condition     = var.directory_type == "not-ad-joined" || var.directory_type == "ad-joined"
    error_message = "The directory_type must be set to not-ad-joined or ad-joined. We do not support AADDS at this time."
  }
}

variable "soft_delete_retention_policy" {
  type        = bool
  description = "Is soft delete enabled for containers and blobs?"
  default     = false
}
