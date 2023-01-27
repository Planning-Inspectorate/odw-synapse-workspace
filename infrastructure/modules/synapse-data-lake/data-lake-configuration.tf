resource "azurerm_storage_blob" "config_files" {
  for_each = toset(var.data_lake_config_files)

  name                   = each.key
  content_md5            = filemd5("${var.data_lake_config_files_path}/${each.key}")
  source                 = "${var.data_lake_config_files_path}/${each.key}"
  storage_account_name   = azurerm_storage_account.synapse.name
  storage_container_name = var.data_lake_config_container_name
  type                   = "Block"

  depends_on = [
    azurerm_storage_container.synapse
  ]
}
