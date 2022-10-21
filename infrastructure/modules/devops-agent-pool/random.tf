resource "random_password" "devops_agent_password" {
  count = var.deploy_agent_pool ? 1 : 0

  length           = 32
  special          = true
  override_special = "#&-_+"
  min_lower        = 2
  min_upper        = 2
  min_numeric      = 2
  min_special      = 2
}
