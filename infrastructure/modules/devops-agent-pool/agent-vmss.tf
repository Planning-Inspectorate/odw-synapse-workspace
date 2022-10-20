resource "azurerm_linux_virtual_machine_scale_set" "devops_agent_pool" {
  #checkov:skip=CKV_AZURE_49: SSH authentication not required
  #checkov:skip=CKV_AZURE_97: Host encryption not required
  #checkov:skip=CKV_AZURE_149: Password authentication required
  count = var.deploy_agent_pool ? 1 : 0

  name                = "pins-vmss-devops-${local.resource_suffix}"
  resource_group_name = azurerm_resource_group.devops_agents.name
  location            = var.location
  sku                 = var.devops_agent_vm_sku
  instances           = var.devops_agent_instances

  overprovision          = false
  single_placement_group = false

  admin_username                  = var.devops_agent_username
  admin_password                  = random_password.devops_agent_password[0].result
  disable_password_authentication = false

  platform_fault_domain_count = 1

  source_image_id = data.azurerm_image.azure_agents.id

  boot_diagnostics {
    storage_account_uri = null
  }

  network_interface {
    enable_accelerated_networking = true
    name                          = "pins-vmss-nic-devops-${local.resource_suffix}"
    primary                       = true

    ip_configuration {
      name      = "ipconfig"
      primary   = true
      subnet_id = var.vnet_subnet_ids[var.devops_agent_subnet_name]
    }
  }

  os_disk {
    caching              = "ReadOnly"
    storage_account_type = "Standard_LRS"

    diff_disk_settings {
      option = "Local"
    }
  }

  lifecycle {
    ignore_changes = [
      automatic_instance_repair,
      automatic_os_upgrade_policy,
      extension,
      instances,
      tags
    ]
  }
}
