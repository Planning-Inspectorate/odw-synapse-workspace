resource "azurerm_private_endpoint" "odt_backoffice_servicebus_private_endpoint" {
  name                = "pins-pe-backoffice-sb-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  subnet_id           = var.synapse_private_endpoint_vnet_subnets[var.synapse_private_endpoint_subnet_name]
  private_service_connection {
    name                           = "pins-psc-backoffice-sb-${local.resource_suffix}"
    is_manual_connection           = false
    private_connection_resource_id = data.azurerm_resources.odt_pe_backoffice_sb.resources[0].id
    subresource_names              = ["namespace"]
  }
  private_dns_zone_group {
    name                 = "pins-pdns-backoffice-sb-${local.resource_suffix}"
    private_dns_zone_ids = [var.odt_back_office_private_endpoint_dns_zone_id]
  }

  tags = local.tags
}