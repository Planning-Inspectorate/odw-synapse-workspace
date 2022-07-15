data_lake_account_tier     = "Standard"
data_lake_replication_type = "GRS"
data_lake_role_assignments = {
  "Storage Blob Data Contributor" = "ebcc4498-4abe-4457-8970-7fa08bf87543" # pins-odw-dev-administrators
  "Storage Blob Data Contributor" = "48bd5755-6d7d-4a17-b044-7522c54e9c7d" # pins-odw-dev-dataengineers
}

data_lake_storage_containers = [
  "odw-curated",
  "odw-raw",
  "odw-standardised",
  "odw-harmonised",
  "odw-config"
]

environment = "test"
location    = "uk-south"

key_vault_role_assignments = {
  "Key Vault Administrator"   = "ebcc4498-4abe-4457-8970-7fa08bf87543" # pins-odw-dev-administrators
  "Key Vault Secrets Officer" = "48bd5755-6d7d-4a17-b044-7522c54e9c7d" # pins-odw-dev-dataengineers
}

network_watcher_enabled = false

spark_pool_enabled        = true
spark_pool_max_node_count = 12
spark_pool_min_node_count = 3
spark_pool_node_size      = "Small"
spark_pool_version        = "2.4"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = true
sql_pool_sku_name  = "DW100c"

synapse_aad_administrator = {
  username  = "pins-odw-data-dev-syn-ws-sqladmins"
  object_id = "1c996957-30e4-40fe-b0b4-82d40f13c058"
}

synapse_github_details = {}
synapse_github_enabled = false

# synapse_github_details = {
#   account_name    = "Planning-Inspectorate"
#   branch_name     = "main"
#   repository_name = "ODW-Service"
#   root_folder     = "/workspace"
# }

synapse_sql_administrator_username = "synadmin"
synapse_role_assignments = {
  "Synapse Administrator"    = "6a38f212-3834-4e2e-93fb-f81bb3a3fe49" # pins-odw-data-dev-syn-ws-administrators
  "Synapse Contributor"      = "0a5073e3-b8e9-4786-8e1f-39f2c277aeb2" # pins-odw-data-dev-syn-ws-contributors
  "Synapse Compute Operator" = "a66ee73a-c31b-451d-b13e-19b4e92c0c25" # pins-odw-data-dev-syn-ws-computeoperators
}

tags = {}

vnet_base_cidr_block = "10.80.0.0/24"
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
