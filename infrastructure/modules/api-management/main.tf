resource "azurerm_api_management" "api_management" {
  #checkov:skip=CKV_AZURE_174: "Ensure API management public access is disabled"
  name                = "pins-apim-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  publisher_name      = var.publisher_name
  publisher_email     = var.publisher_email

  sku_name = var.sku_name

  virtual_network_configuration {
    subnet_id = var.synapse_vnet_subnet_names[var.synapse_apim_subnet_name]
  }

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_api_management_product" "api_management" {
  product_id            = "demo-product"
  api_management_name   = azurerm_api_management.api_management.name
  resource_group_name   = var.resource_group_name
  display_name          = "Demo Product"
  description           = "Demo Product part of POC"
  subscription_required = false
  approval_required     = false
  published             = false
}

resource "azurerm_api_management_api" "api_management" {
  name                  = "api_management_demo_api"
  resource_group_name   = var.resource_group_name
  api_management_name   = azurerm_api_management.api_management.name
  revision              = "1"
  display_name          = "Demo Conference API"
  path                  = "api_management"
  protocols             = ["https"]
  service_url           = "https://conferenceapi.azurewebsites.net/"
  subscription_required = false


  import {
    content_format = "openapi+json"
    content_value  = file("${path.module}/Demo_Conference_API.openapi+json.json")
  }
}

resource "azurerm_api_management_product_api" "api_management" {
  api_name            = azurerm_api_management_api.api_management.name
  product_id          = azurerm_api_management_product.api_management.product_id
  api_management_name = azurerm_api_management.api_management.name
  resource_group_name = var.resource_group_name
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
