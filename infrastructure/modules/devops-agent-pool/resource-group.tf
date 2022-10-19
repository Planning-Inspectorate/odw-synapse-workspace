resource "azurerm_resource_group" "devops_agents" {
  name     = "pins-rg-devops-${local.resource_suffix}"
  location = var.location

  tags = local.tags
}
