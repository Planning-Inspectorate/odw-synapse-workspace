terraform {
  required_version = ">= 1.0"
  
  required_providers {
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.0"
    }
    github = {
      source  = "integrations/github"
      version = "~> 5.0"
    }
  }
}

# Configure the Azure AD provider
provider "azuread" {
  # Authentication will use Azure CLI credentials
}

# Configure the GitHub provider
provider "github" {
  owner = "Planning-Inspectorate"
  # Token should be provided via GITHUB_TOKEN environment variable
}

# Data source to get the existing Azure AD user (stef-solirius)
data "azuread_user" "test_user" {
  user_principal_name = "stefania.deligia@planninginspectorate.gov.uk"
}

# Create an Entra ID Security Group for ODW-Admins (spike test)
resource "azuread_group" "odw_github_admins_spike" {
  display_name     = "ODW-GitHub-Admins-Spike"
  description      = "Spike test: Repository administrators with full access for ODW repositories"
  security_enabled = true
  
  # Add the test user as a member
  members = [
    data.azuread_user.test_user.object_id
  ]
  
  # Add owners if needed (optional)
  # owners = [
  #   data.azuread_user.test_user.object_id
  # ]
}

# Create GitHub team for ODW Admins (spike test)
resource "github_team" "odw_admins_spike" {
  name        = "odw-admins-spike"
  description = "Spike test: Repository administrators with full access for ODW repositories"
  privacy     = "closed"
}

# Sync GitHub team with Entra ID group
# Note: This requires GitHub Enterprise with SAML SSO configured
# For the spike, we'll manually add the user to demonstrate the concept
resource "github_team_membership" "spike_test_member" {
  team_id  = github_team.odw_admins_spike.id
  username = "stef-solirius"
  role     = "member"
}

# Grant the team admin access to the odw-synapse-workspace repository
resource "github_team_repository" "odw_synapse_workspace_spike" {
  team_id    = github_team.odw_admins_spike.id
  repository = "odw-synapse-workspace"
  permission = "admin"
}

# Outputs
output "entraid_group_id" {
  description = "The Object ID of the Entra ID group"
  value       = azuread_group.odw_github_admins_spike.object_id
}

output "entraid_group_name" {
  description = "The display name of the Entra ID group"
  value       = azuread_group.odw_github_admins_spike.display_name
}

output "github_team_id" {
  description = "The ID of the GitHub team"
  value       = github_team.odw_admins_spike.id
}

output "github_team_name" {
  description = "The name of the GitHub team"
  value       = github_team.odw_admins_spike.name
}
