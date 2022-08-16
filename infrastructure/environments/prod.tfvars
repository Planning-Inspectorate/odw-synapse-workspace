bastion_host_enabled = false
bastion_vm_username  = "basadmin"
bastion_vm_size      = "Standard_F2s_v2"

data_lake_account_tier     = "Standard"
data_lake_replication_type = "GZRS"
data_lake_role_assignments = {
  "Storage Blob Data Contributor" = "1fa42635-5dc3-43bc-b5da-77578f3dabb7" # pins-odw-prod-administrators
  "Storage Blob Data Contributor" = "5c56c7a0-6845-43e7-877c-c8dd527107a3" # pins-odw-prod-dataengineers
}

data_lake_storage_containers = [
  "odw-curated",
  "odw-raw",
  "odw-standardised",
  "odw-harmonised",
  "odw-config"
]

environment = "prod"
location    = "uk-south"

key_vault_role_assignments = {
  "Key Vault Administrator"   = "1fa42635-5dc3-43bc-b5da-77578f3dabb7" # pins-odw-prod-administrators
  "Key Vault Secrets Officer" = "5c56c7a0-6845-43e7-877c-c8dd527107a3" # pins-odw-preprod-dataengineers
}

network_watcher_enabled = false

spark_pool_enabled        = true
spark_pool_max_node_count = 12
spark_pool_min_node_count = 3
spark_pool_node_size      = "Small"
spark_pool_version        = "3.2"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = true
sql_pool_sku_name  = "DW100c"

sql_server_administrator_username = "sqladmin"
sql_server_enabled                = false

synapse_aad_administrator = {
  username  = "pins-odw-data-prod-syn-ws-sqladmins"
  object_id = "f0e4d89f-3288-48c9-ada9-1227a069c76e"
}

synapse_sql_administrator_username = "synadmin"
synapse_role_assignments = {
  "Synapse Administrator"    = "a2568721-f55c-4cbe-8cef-3d4fa2e1cee7" # pins-odw-data-prod-syn-ws-administrators
  "Synapse Contributor"      = "76259388-176a-4db7-a5b7-db2861ef7220" # pins-odw-data-prod-syn-ws-contributors
  "Synapse Compute Operator" = "df8e79ba-3f7b-457c-936a-dada88cb178a" # pins-odw-data-prod-syn-ws-computeoperators
}

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
    "name" : "ComputeSubnet"
    "new_bits" : 2 # /26
  },
  {
    "name" : null, # Reserved
    "new_bits" : 2 # /26
  }
]
