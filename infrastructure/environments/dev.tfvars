alert_group_platform_enabled             = true
alert_group_platform_recipients          = ["nasir.rahman@planninginspectorate.gov.uk"]
alert_group_synapse_enabled              = true
alert_group_synapse_recipients           = ["abdullah.pakwashee@planninginspectorate.gov.uk"]
alert_scope_service_health               = "/subscriptions/ff442a29-fc06-4a13-8e3e-65fd5da513b3"
alert_threshold_data_lake_capacity_bytes = 10995116277760 # 10TiB

bastion_host_enabled = true
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
data_lake_retention_days        = 7
data_lake_role_assignments = {
  "Storage Blob Data Contributor" = [
    "ebcc4498-4abe-4457-8970-7fa08bf87543", # pins-odw-dev-administrators
    "48bd5755-6d7d-4a17-b044-7522c54e9c7d", # pins-odw-dev-dataengineers
    "b4dbfba4-b78b-4163-9b39-87ea03e2d5ed"  # planninginspectorate-operational-data-warehouse-ff442a29-fc06-4a13-8e3e-65fd5da513b3
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

devops_agent_pool_resource_group_name          = "pins-rg-devops-odw-dev-uks"
devops_agent_pool_resource_group_name_failover = "pins-rg-devops-odw-dev-ukw"

environment = "dev"
location    = "uk-south"

key_vault_role_assignments = {
  "Key Vault Administrator" = [
    "ebcc4498-4abe-4457-8970-7fa08bf87543" # pins-odw-dev-administrators
  ],
  "Key Vault Secrets Officer" = [
    "48bd5755-6d7d-4a17-b044-7522c54e9c7d" # pins-odw-dev-dataengineers
  ]
}

network_watcher_enabled = false

odt_back_office_service_bus_failover_enabled             = false
odt_back_office_service_bus_resource_group_name          = "pins-sb-back-office-dev-ukw-001"
odt_back_office_service_bus_resource_group_name_failover = "pins-sb-back-office-dev-uks-001"
odt_back_office_subscription_id                          = "962e477c-0f3b-4372-97fc-a198a58e259e"

service_bus_failover_enabled = false
service_bus_role_assignments = {
  "Azure Service Bus Data Owner" = [
    "ebcc4498-4abe-4457-8970-7fa08bf87543" # pins-odw-dev-administrators
  ]
}

spark_pool_enabled         = true
spark_pool_max_node_count  = 12
spark_pool_min_node_count  = 3
spark_pool_node_size       = "Small"
spark_pool_timeout_minutes = 60
spark_pool_version         = "3.3"

spark_pool_preview_enabled = true
spark_pool_preview_version = "3.3"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = false
sql_pool_sku_name  = "DW100c"

sql_server_administrator_username = "sqladmin"
sql_server_enabled                = true

synapse_aad_administrator = {
  username  = "pins-odw-data-dev-syn-ws-sqladmins"
  object_id = "1c996957-30e4-40fe-b0b4-82d40f13c058"
}

synapse_data_exfiltration_enabled  = false
synapse_sql_administrator_username = "synadmin"
synapse_role_assignments = {
  "Synapse Administrator" = [
    "6a38f212-3834-4e2e-93fb-f81bb3a3fe49", # pins-odw-data-dev-syn-ws-administrators
    "b4dbfba4-b78b-4163-9b39-87ea03e2d5ed"  # planninginspectorate-operational-data-warehouse-ff442a29-fc06-4a13-8e3e-65fd5da513b3
  ],
  "Synapse Contributor" = [
    "0a5073e3-b8e9-4786-8e1f-39f2c277aeb2" # pins-odw-data-dev-syn-ws-contributors
  ],
  "Synapse Compute Operator" = [
    "a66ee73a-c31b-451d-b13e-19b4e92c0c25" # pins-odw-data-dev-syn-ws-computeoperators
  ]
}

tags = {}

tenant_id = "5878df98-6f88-48ab-9322-998ce557088d"

vnet_base_cidr_block          = "10.70.0.0/24"
vnet_base_cidr_block_failover = "10.70.1.0/24"
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
