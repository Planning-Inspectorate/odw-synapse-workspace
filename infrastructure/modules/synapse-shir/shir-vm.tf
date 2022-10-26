resource "azurerm_network_interface" "shir" {
  name                = "pins-vm-nic-shir-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "ipconfig"
    subnet_id                     = var.vnet_subnet_ids[var.devops_agent_subnet_name]
    private_ip_address_allocation = "Dynamic"
  }

  tags = local.tags
}

resource "azurerm_windows_virtual_machine" "synapse" {
  #checkov:skip=CKV_AZURE_50: VM extensions are required for provisioning and updates
  name                       = "pins-vm-shir-${local.resource_suffix}"
  resource_group_name        = var.resource_group_name
  location                   = var.location
  size                       = var.runtime_vm_size
  admin_username             = var.runtime_vm_username
  admin_password             = random_password.shir_vm_administrator_password.result
  computer_name              = "shir-${random_string.unique_id.id}"
  encryption_at_host_enabled = true
  network_interface_ids      = [azurerm_network_interface.shir.id]
  patch_mode                 = contains(split("_", var.runtime_vm_size), "v2") ? "AutomaticByPlatform" : "AutomaticByOS"
  hotpatching_enabled        = contains(split("_", var.runtime_vm_size), "v2") ? true : false

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = var.runtime_vm_image["publisher"]
    offer     = var.runtime_vm_image["offer"]
    sku       = var.runtime_vm_image["sku"]
    version   = var.runtime_vm_image["version"]
  }

  tags = local.tags
}
