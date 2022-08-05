resource "azurerm_network_interface" "jumpbox" {
  name                = "bas-vm-nic-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name

  ip_configuration {
    name                          = "ipconfig"
    subnet_id                     = var.synapse_vnet_subnets[var.synapse_compute_subnet_name]
    private_ip_address_allocation = "Dynamic"
  }

  tags = local.tags
}

resource "azurerm_windows_virtual_machine" "jumpbox" {
  #checkov:skip=CKV_AZURE_50:   SKIP: VM extensions required for hotpatching
  #checkov:skip=CKV_AZURE_151:  TODO: Enable VM encryption
  name                     = "bas-vm-${local.resource_suffix}"
  location                 = var.location
  resource_group_name      = var.resource_group_name
  size                     = var.bastion_vm_size
  admin_username           = var.bastion_vm_username
  admin_password           = random_password.bastion_vm_admin_password.result
  computer_name            = "bastion-${random_string.unique_id.id}"
  enable_automatic_updates = true
  network_interface_ids    = [azurerm_network_interface.jumpbox.id]
  patch_mode               = "AutomaticByPlatform"
  hotpatching_enabled      = true

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  # source_image_reference {
  #   publisher = "MicrosoftWindowsServer"
  #   offer     = "WindowsServer"
  #   sku       = "2022-datacenter-azure-edition-core"
  #   version   = "latest"
  # }

  source_image_reference {
    publisher = "MicrosoftWindowsDesktop"
    offer     = "windows-11"
    sku       = "win11-21h2-ent"
    version   = "latest"
  }

  tags = local.tags
}
