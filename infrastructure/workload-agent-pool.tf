module "devops_agent_pool" {
  source = "./modules/devops-agent-pool"

  environment  = var.environment
  location     = module.azure_region.location_cli
  service_name = local.service_name

  deploy_agent_pool         = var.deploy_agent_pool
  devops_agent_image_prefix = var.devops_agent_image_prefix
  devops_agent_subnet_name  = local.compute_subnet_name
  resource_group_name       = var.devops_agent_pool_resource_group_name
  vnet_subnet_ids           = module.synapse_network.vnet_subnets

  depends_on = [
    module.synapse_network
  ]

  tags = local.tags
}

module "devops_agent_pool_failover" {
  count = var.failover_deployment ? 1 : 0

  source = "./modules/devops-agent-pool"

  environment  = var.environment
  location     = module.azure_region.location_cli
  service_name = local.service_name

  deploy_agent_pool         = var.deploy_agent_pool
  devops_agent_image_prefix = var.devops_agent_image_prefix
  devops_agent_subnet_name  = local.compute_subnet_name
  devops_agent_vm_sku       = var.devops_agent_vm_sku
  resource_group_name       = var.devops_agent_pool_resource_group_name_failover
  vnet_subnet_ids           = module.synapse_network_failover.vnet_subnets

  depends_on = [
    module.synapse_network_failover
  ]

  tags = local.tags
}
