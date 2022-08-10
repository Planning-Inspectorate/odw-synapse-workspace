resource "azurerm_synapse_spark_pool" "synapse" {
  count = var.spark_pool_enabled ? 1 : 0

  name                 = "pinssynspodw"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  node_size_family     = "MemoryOptimized"
  node_size            = var.spark_pool_node_size
  spark_version        = var.spark_pool_version

  auto_pause {
    delay_in_minutes = 15
  }

  auto_scale {
    max_node_count = var.spark_pool_max_node_count
    min_node_count = var.spark_pool_min_node_count
  }

  tags = local.tags
}
