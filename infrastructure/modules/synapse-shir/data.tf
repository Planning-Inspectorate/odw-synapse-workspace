data "azurerm_client_config" "current" {}

data "azurerm_storage_account_sas" "shir" {
  connection_string = azurerm_storage_account.shir.primary_connection_string
  https_only        = true
  signed_version    = "2021-06-08"

  start  = timestamp()
  expiry = timeadd(timestamp(), "30m")

  permissions {
    read    = true
    write   = false
    delete  = false
    list    = false
    add     = false
    create  = false
    update  = false
    process = false
    tag     = false
    filter  = false
  }

  resource_types {
    service   = true
    container = true
    object    = true
  }

  services {
    blob  = true
    queue = false
    table = false
    file  = false
  }
}
