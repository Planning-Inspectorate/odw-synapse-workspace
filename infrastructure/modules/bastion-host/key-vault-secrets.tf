resource "azurerm_key_vault_secret" "bastion_vm_admin_password" {
  content_type    = "text/plain"
  key_vault_id    = var.key_vault_id
  name            = "bastion-vm-admin-password"
  value           = random_password.bastion_vm_admin_password.result
  expiration_date = timeadd(timestamp(), "867834h")

  lifecycle {
    ignore_changes = [
      expiration_date,
      value
    ]
  }
}
