alert_group_platform_enabled             = true
alert_group_platform_recipients          = ["odw_support@planninginspectorate.gov.uk"]
alert_group_synapse_enabled              = true
alert_group_synapse_recipients           = ["odw_support@planninginspectorate.gov.uk"]
alert_scope_service_health               = "/subscriptions/a82fd28d-5989-4e06-a0bb-1a5d859f9e0c"
alert_threshold_data_lake_capacity_bytes = 10995116277760 # 10TiB

bastion_host_enabled = false
bastion_vm_username  = "basadmin"
bastion_vm_size      = "Standard_F2s_v2"
bastion_vm_image = {
  publisher = "microsoft-dsvm"
  offer     = "dsvm-win-2019"
  sku       = "winserver-2019"
  version   = "latest"
}

data_lake_account_tier          = "Standard"
data_lake_config_container_name = "odw-config"
data_lake_replication_type      = "GRS"
data_lake_retention_days        = 28
data_lake_role_assignments = {
  "Storage Blob Data Contributor" = [
    "1fa42635-5dc3-43bc-b5da-77578f3dabb7", # pins-odw-prod-administrators
    "5c56c7a0-6845-43e7-877c-c8dd527107a3", # pins-odw-prod-dataengineers
    "d1761ac5-c65f-4b48-bee9-a2179b989adc"  # planninginspectorate-operational-data-warehouse-a82fd28d-5989-4e06-a0bb-1a5d859f9e0c
  ]
}
data_lake_storage_containers = [
  "backup-logs",
  "odw-curated",
  "odw-raw",
  "odw-standardised",
  "odw-harmonised",
  "odw-config"
]

devops_agent_pool_resource_group_name          = "pins-rg-devops-odw-prod-uks"
devops_agent_pool_resource_group_name_failover = "pins-rg-devops-odw-prod-ukw"

environment = "prod"
location    = "uk-south"

key_vault_role_assignments = {
  "Key Vault Administrator" = [
    "1fa42635-5dc3-43bc-b5da-77578f3dabb7" # pins-odw-prod-administrators
  ],
  "Key Vault Secrets Officer" = [
    "5c56c7a0-6845-43e7-877c-c8dd527107a3" # pins-odw-prod-dataengineers
  ]
}

network_watcher_enabled = false

odt_back_office_service_bus_enabled                      = true
odt_back_office_service_bus_failover_enabled             = false
odt_back_office_service_bus_name                         = "pins-sb-back-office-prod-ukw-001"
odt_back_office_service_bus_name_failover                = "pins-sb-back-office-prod-uks-001"
odt_back_office_service_bus_resource_group_name          = "pins-rg-back-office-prod-ukw-001"
odt_back_office_service_bus_resource_group_name_failover = "pins-rg-back-office-prod-uks-001"
odt_back_office_subscription_id                          = "d1d6c393-2fe3-40af-ac27-f5b6bad36735"

service_bus_failover_enabled = true
service_bus_role_assignments = {
  "Azure Service Bus Data Owner" = [
    "1fa42635-5dc3-43bc-b5da-77578f3dabb7" # pins-odw-prod-administrators
  ]
}

spark_pool_enabled         = true
spark_pool_max_node_count  = 12
spark_pool_min_node_count  = 3
spark_pool_node_size       = "Small"
spark_pool_timeout_minutes = 15
spark_pool_version         = "3.3"

spark_pool_preview_enabled = true
spark_pool_preview_version = "3.3"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = false
sql_pool_sku_name  = "DW100c"

sql_server_administrator_username = "sqladmin"
sql_server_enabled                = false

synapse_aad_administrator = {
  username  = "pins-odw-data-prod-syn-ws-sqladmins"
  object_id = "f0e4d89f-3288-48c9-ada9-1227a069c76e"
}

synapse_data_exfiltration_enabled  = false
synapse_sql_administrator_username = "synadmin"
synapse_role_assignments = {
  "Synapse Administrator" = [
    "a2568721-f55c-4cbe-8cef-3d4fa2e1cee7", # pins-odw-data-prod-syn-ws-administrators
    "d1761ac5-c65f-4b48-bee9-a2179b989adc"  # planninginspectorate-operational-data-warehouse-a82fd28d-5989-4e06-a0bb-1a5d859f9e0c
  ],
  "Synapse Contributor" = [
    "76259388-176a-4db7-a5b7-db2861ef7220" # pins-odw-data-prod-syn-ws-contributors
  ],
  "Synapse Compute Operator" = [
    "df8e79ba-3f7b-457c-936a-dada88cb178a" # pins-odw-data-prod-syn-ws-computeoperators
  ]
}

tags = {}

tenant_id = "5878df98-6f88-48ab-9322-998ce557088d"

vnet_base_cidr_block          = "10.90.0.0/24"
vnet_base_cidr_block_failover = "10.90.1.0/24"
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
