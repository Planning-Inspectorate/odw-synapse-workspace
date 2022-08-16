resource "random_string" "unique_id" {
  length  = 6
  upper   = false
  special = false
}

resource "random_password" "sql_server_administrator_password" {
  length           = 32
  special          = true
  override_special = "#&-_+"
  min_lower        = 2
  min_upper        = 2
  min_numeric      = 2
  min_special      = 2
}
