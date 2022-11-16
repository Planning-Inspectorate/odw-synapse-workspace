build {
  name = "azure-devops-agents"

  source "source.azure-arm.azure-agents" {
    managed_image_resource_group_name = var.resource_group_name
    managed_image_name                = "${var.image_prefix}-${formatdate("YYYYMMDDhhmmss",timestamp())}"

    os_type         = "Linux"
    image_publisher = "canonical"
    image_offer     = "0001-com-ubuntu-server-focal"
    image_sku       = "20_04-lts"

    location        = var.location
    vm_size         = "Standard_D2ds_v5"
  }

  provisioner "shell" {
    execute_command = "chmod +x {{ .Path }}; {{ .Vars }} sudo -E bash -e '{{ .Path }}'"
    script          = "${path.cwd}/tools.sh"
  }
}
