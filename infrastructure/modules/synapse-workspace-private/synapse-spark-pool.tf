resource "azurerm_synapse_spark_pool" "synapse_preview" {
  #checkov:skip=CKV_AZURE_242: Ensure isolated compute is enabled for Synapse Spark pools (checkov v3)
  count = var.spark_pool_preview_enabled ? 1 : 0

  name                           = "pinssynspodwpr"
  synapse_workspace_id           = azurerm_synapse_workspace.synapse.id
  node_size_family               = "MemoryOptimized"
  node_size                      = var.spark_pool_node_size
  spark_version                  = var.spark_pool_preview_version
  session_level_packages_enabled = true

  auto_pause {
    delay_in_minutes = var.spark_pool_timeout_minutes
  }

  auto_scale {
    max_node_count = var.spark_pool_max_node_count
    min_node_count = var.spark_pool_min_node_count
  }

  dynamic "library_requirement" {
    for_each = var.spark_pool_preview_requirements != null ? [1] : []

    content {
      content  = var.spark_pool_preview_requirements
      filename = "requirements-preview.txt"
    }
  }

  spark_config {
    content  = <<-EOT
      spark.executorEnv.dataLakeAccountName ${var.data_lake_account_name}
      spark.executorEnv.keyVaultName ${var.key_vault_name}
      EOT
    filename = "configuration.txt"
  }

  tags = local.tags

}

resource "azurerm_synapse_spark_pool" "synapse34" {
  #checkov:skip=CKV_AZURE_242: Ensure isolated compute is enabled for Synapse Spark pools (checkov v3)
  count = var.spark_pool_enabled ? 1 : 0

  name                           = "pinssynspodw34"
  synapse_workspace_id           = azurerm_synapse_workspace.synapse.id
  node_size_family               = "MemoryOptimized"
  node_size                      = var.spark_pool_node_size
  spark_version                  = var.new_spark_pool_version
  session_level_packages_enabled = true

  auto_pause {
    delay_in_minutes = var.spark_pool_timeout_minutes
  }

  auto_scale {
    max_node_count = var.spark_pool_max_node_count
    min_node_count = var.spark_pool_min_node_count
  }

  dynamic "library_requirement" {
    for_each = var.spark_pool_requirements != null ? [1] : []

    content {
      content  = var.spark_pool_preview_requirements
      filename = "requirements-preview.txt"
    }
  }

  spark_config {
    content  = <<-EOT
      spark.executorEnv.dataLakeAccountName ${var.data_lake_account_name}
      spark.executorEnv.keyVaultName ${var.key_vault_name}
      spark.sql.parquet.int96RebaseModeInWrite CORRECTED
      spark.sql.constraintPropagation.enabled false
      EOT
    filename = "configuration.txt"
  }

  tags = local.tags
}