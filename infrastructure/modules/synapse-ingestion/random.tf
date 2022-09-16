resource "random_string" "unique_id" {
  length  = 6
  upper   = false
  special = false
}
