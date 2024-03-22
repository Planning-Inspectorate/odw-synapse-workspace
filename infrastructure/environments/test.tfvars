alert_group_platform_enabled             = true
alert_group_synapse_enabled              = true
alert_scope_service_health               = "/subscriptions/6b18ba9d-2399-48b5-a834-e0f267be122d"
alert_threshold_data_lake_capacity_bytes = 10995116277760 # 10TiB

apim_enabled         = false
apim_publisher_email = "alex.delany@planninginspectorate.gov.uk"
apim_publisher_name  = "Alex Delany"
apim_sku_name        = "Developer_1"

bastion_host_enabled = false
bastion_vm_username  = "basadmin"
bastion_vm_size      = "Standard_F2s_v2"
bastion_vm_image = {
  publisher = "microsoft-dsvm"
  offer     = "dsvm-win-2019"
  sku       = "winserver-2019"
  version   = "latest"
}

data_lake_account_tier     = "Standard"
data_lake_replication_type = "GRS"
data_lake_retention_days   = 7
data_lake_role_assignments = {
  "Storage Blob Data Contributor" = [
    "8274feca-09ef-41b1-9b4e-5eedc3384df4", # pins-odw-preprod-administrators
    "7c906e1b-ffbb-44d3-89a1-6772b9c9c148", # pins-odw-preprod-dataengineers
    "9d4c68d1-c43d-4502-b35f-74f31c497757"  # Azure DevOps Pipelines - ODW Test - Infrastructure
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

devops_agent_pool_resource_group_name          = "pins-rg-devops-odw-test-uks"
devops_agent_pool_resource_group_name_failover = "pins-rg-devops-odw-test-ukw"

environment = "test"

function_app_enabled = true
function_app = [
  {
    name = "fnapp01"
    site_config = {
      application_stack = {
        python_version = "3.11"
      }
    }
  }
]

location = "uk-south"

logic_app_enabled = false

key_vault_role_assignments = {
  "Key Vault Administrator" = [
    "8274feca-09ef-41b1-9b4e-5eedc3384df4" # pins-odw-preprod-administrators
  ],
  "Key Vault Secrets Officer" = [
    "7c906e1b-ffbb-44d3-89a1-6772b9c9c148" # pins-odw-preprod-dataengineers
  ]
}

message_storage_account = "https://pinsstodwtestukswic3ai.blob.core.windows.net"

message_storage_container = "odw-raw/ServiceBus"

network_watcher_enabled = false

odt_back_office_service_bus_enabled                      = true
odt_back_office_service_bus_failover_enabled             = false
odt_back_office_service_bus_name                         = "pins-sb-back-office-test-ukw-001"
odt_back_office_service_bus_name_failover                = "pins-sb-back-office-test-uks-001"
odt_back_office_service_bus_resource_group_name          = "pins-rg-back-office-test-ukw-001"
odt_back_office_service_bus_resource_group_name_failover = "pins-rg-back-office-test-uks-001"
odt_backoffice_sb_topic_subscriptions = [
  {
    subscription_name = "service-user"
    topic_name        = "service-user"
  },
  {
    subscription_name = "nsip-project"
    topic_name        = "nsip-project"
  },
  {
    subscription_name = "nsip-exam-timetable"
    topic_name        = "nsip-exam-timetable"
  },
  {
    subscription_name = "nsip-document"
    topic_name        = "nsip-document"
  },
  {
    subscription_name = "nsip-representation"
    topic_name        = "nsip-representation"
  },
  {
    subscription_name = "nsip-s51-advice"
    topic_name        = "nsip-s51-advice"
  },
  {
    subscription_name = "nsip-project-update"
    topic_name        = "nsip-project-update"
  },
  {
    subscription_name = "nsip-subscription"
    topic_name        = "nsip-subscription"
  },
  {
    subscription_name = "folder"
    topic_name        = "folder"
  }
]


service_bus_failover_enabled = false
service_bus_role_assignments = {
  "Azure Service Bus Data Owner" = {
    groups = ["pins-odw-preprod-administrators"]
  }
}

service_bus_topics_and_subscriptions = [
  {
    name = "employee"
    subscriptions = {
      "employee"        = {},
      "employee-verify" = {}
    }
  },
  {
    name = "zendesk"
    subscriptions = {
      "zendesk"        = {},
      "zendesk-verify" = {}
    }
  },
  {
    name = "service-user"
    subscriptions = {
      "service-user" = {},
    }
  },
  {
    name = "nsip-project"
    subscriptions = {
      "nsip-project" = {},
    }
  },
  {
    name = "nsip-exam-timetable"
    subscriptions = {
      "nsip-exam-timetable" = {},
    }
  },
  {
    name = "nsip-document"
    subscriptions = {
      "nsip-document" = {},
    }
  },
  {
    name = "nsip-representation"
    subscriptions = {
      "nsip-representation" = {},
    }
  },
  {
    name = "nsip-s51-advice"
    subscriptions = {
      "nsip-s51-advice" = {},
    }
  },
]

spark_pool_enabled         = true
spark_pool_max_node_count  = 12
spark_pool_min_node_count  = 3
spark_pool_node_size       = "Small"
spark_pool_timeout_minutes = 60
spark_pool_version         = "3.3"

spark_pool_preview_enabled = true
spark_pool_preview_version = "3.2"

sql_pool_collation = "SQL_Latin1_General_CP1_CI_AS"
sql_pool_enabled   = false
sql_pool_sku_name  = "DW100c"

sql_server_administrator_username = "sqladmin"
sql_server_enabled                = false

synapse_aad_administrator = {
  username  = "pins-odw-data-preprod-syn-ws-sqladmins"
  object_id = "ba5af92f-a1bf-4332-a3c9-613a0a8f1b12"
}

synapse_data_exfiltration_enabled  = false
synapse_sql_administrator_username = "synadmin"
synapse_role_assignments = {
  "Synapse Administrator" = [
    "be52cb0c-858f-4698-8c40-3a5ec793a2e3", # pins-odw-data-preprod-syn-ws-administrators
    "9d4c68d1-c43d-4502-b35f-74f31c497757"  # Azure DevOps Pipelines - ODW Test - Infrastructure
  ],
  "Synapse Contributor" = [
    "d59a3e85-58db-4b70-8f88-3f4a4a82ee27" # pins-odw-data-preprod-syn-ws-contributors
  ],
  "Synapse Compute Operator" = [
    "f9c580cd-cab0-4c49-9f50-290194ade29e" # pins-odw-data-preprod-syn-ws-computeoperators
  ]
}

tags = {}

tenant_id = "5878df98-6f88-48ab-9322-998ce557088d"

vnet_base_cidr_block          = "10.80.0.0/24"
vnet_base_cidr_block_failover = "10.80.1.0/24"
vnet_subnets = [
  {
    "name" : "AzureBastionSubnet",
    "new_bits" : 4 # /28
    service_endpoints  = []
    service_delegation = []
  },
  {
    "name" : "FunctionAppSubnet",
    "new_bits" : 4 # /28
    service_endpoints = ["Microsoft.Storage", "Microsoft.KeyVault", "Microsoft.ServiceBus"]
    service_delegation = [
      {
        delegation_name = "Microsoft.Web/serverFarms"
        actions         = ["Microsoft.Network/virtualNetworks/subnets/action"]
      }
    ]
  },
  {
    "name" : "SynapseEndpointSubnet",
    "new_bits" : 2 # /26
    service_endpoints  = []
    service_delegation = []
  },
  {
    "name" : "ComputeSubnet"
    "new_bits" : 2 # /26
    service_endpoints  = ["Microsoft.Storage", "Microsoft.KeyVault"]
    service_delegation = []
  },
  {
    "name" : "ApimSubnet",
    "new_bits" : 2 # /26
    service_endpoints  = []
    service_delegation = []
  },
]
