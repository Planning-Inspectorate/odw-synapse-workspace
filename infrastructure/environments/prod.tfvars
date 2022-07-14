data_lake_account_tier     = "Standard"
data_lake_replication_type = "GRS"
data_lake_role_assignments = {}
data_lake_storage_containers = [
  "odw-curated",
  "odw-raw",
  "odw-standardised",
  "odw-harmonised",
  "odw-config"
]

environment = "dev"
location    = "uk-south"

key_vault_role_assignments = {}

# key_vault_role_assignments = {
#   "Key Vault Administrator"   =
#   "Key Vault Secrets Officer" =
#   "Key Vault Secrets User"    =
# }

network_watcher_enabled = false

spark_pool_enabled        = true
spark_pool_max_node_count = 12
spark_pool_min_node_count = 3
spark_pool_node_size      = "Small"
spark_pool_version        = "2.4"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = true
sql_pool_sku_name  = "DW100c"

synapse_github_details = {}
synapse_github_enabled = false

# synapse_github_details = {
#   account_name    = "Planning-Inspectorate"
#   branch_name     = "main"
#   repository_name = "ODW-Service"
#   root_folder     = "/workspace"
# }

synapse_role_assignments           = {}
synapse_sql_administrator_username = "synadmin"

# synapse_role_assignments           = {
#   "Synapse Administrator"    = ""
#   "Synapse Contributor"      = ""
#   "Synapse Compute Operator" = ""
# }

tags = {}

vnet_base_cidr_block = "10.90.0.0/24"
vnet_subnets = [
  {
    "name" : "AzureBastionSubnet",
    "new_bits" : 2 # /26
  },
  {
    "name" : "SynapseEndpointSubnet",
    "new_bits" : 2 # /26
  },
  {
    "name" : null, # Reserved
    "new_bits" : 2 # /26
  },
  {
    "name" : null, # Reserved
    "new_bits" : 2 # /26
  }
]
