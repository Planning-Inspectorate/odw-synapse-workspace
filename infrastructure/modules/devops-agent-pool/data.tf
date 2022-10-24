data "azurerm_image" "azure_agents" {
  count = var.deploy_agent_pool ? 1 : 0

  name_regex          = var.devops_agent_image_prefix
  sort_descending     = true
  resource_group_name = azurerm_resource_group.devops_agents.name
}
