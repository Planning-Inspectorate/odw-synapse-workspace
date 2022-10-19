resource "azurerm_resource_group" "devops_agents" {
  name     = var.resource_group_name
  location = var.location

  tags = local.tags
}
