resource "azurerm_logic_app_workflow" "workflows" {
  for_each = var.workflow_names

  name                = each.value
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = local.tags
}
