resource "azurerm_api_management" "api_management" {
  name                = "pins-apim-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  publisher_name      = var.publisher_name
  publisher_email     = var.publisher_email

  sku_name = var.sku_name

  #public_network_access_enabled = false
  virtual_network_configuration {
    subnet_id = var.synapse_vnet_subnet_names[var.synapse_apim_subnet_name]
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

# Create Application Insights
# resource "azurerm_application_insights" "ai" {
#   name                = local.appInsightsName
#   resource_group_name = azurerm_resource_group.rg.name
#   location            = azurerm_resource_group.rg.location
#   application_type    = "web"
#   tags                = var.tags
# }
# # Create Logger
# resource "azurerm_api_management_logger" "apimLogger" {
#   name                = "${local.apimName}-logger"
#   api_management_name = azurerm_api_management.apim.name
#   resource_group_name = data.azurerm_resource_group.rg.name

#   application_insights {
#     instrumentation_key = azurerm_application_insights.ai.instrumentation_key
#   }
# }
