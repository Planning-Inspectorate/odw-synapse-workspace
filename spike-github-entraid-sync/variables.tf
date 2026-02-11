variable "test_user_email" {
  description = "Email address of the test user in Entra ID"
  type        = string
  default     = "ramana.varagani@planninginspectorate.gov.uk"
}

variable "test_user_github" {
  description = "GitHub username of the test user"
  type        = string
  default     = "raamvar"
}

variable "github_org" {
  description = "GitHub organization name"
  type        = string
  default     = "Planning-Inspectorate"
}

variable "repository_name" {
  description = "Repository to grant access to"
  type        = string
  default     = "odw-synapse-workspace"
}

variable "permission_level" {
  description = "Permission level to grant (pull, triage, push, maintain, admin)"
  type        = string
  default     = "admin"
}
