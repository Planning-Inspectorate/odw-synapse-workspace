<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
resource "azurerm_api_management" "api_management" {
=======
resource "azurerm_api_management" "example" {
>>>>>>> apim first commit
=======
resource "azurerm_api_management" "api_management" {
>>>>>>> Updated syntax
=======
resource "azurerm_api_management" "api_management" {
>>>>>>> d71dcb5a7f09d48dcd5bf9c7c62bcc22e96b17ed
  name                = "pins-apim-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  publisher_name      = var.publisher_name
  publisher_email     = var.publisher_email

  sku_name = var.sku_name

  public_network_access_enabled = false
  virtual_network_configuration {
    subnet_id = var.synapse_vnet_subnet_names[var.synapse_apim_subnet_name]
  }

<<<<<<< HEAD
<<<<<<< HEAD
  public_network_access_enabled = false
  virtual_network_configuration {
    subnet_id = var.synapse_vnet_subnet_names[var.synapse_apim_subnet_name]
  }

  tags = local.tags
}
=======
  tags = local.tags
}
<<<<<<< HEAD
>>>>>>> apim first commit
=======
>>>>>>> Updated syntax
=======
  tags = local.tags
}
>>>>>>> d71dcb5a7f09d48dcd5bf9c7c62bcc22e96b17ed
