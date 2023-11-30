output "storage_id" {
  description = "Output of the storage account resource id"
  value       = azurerm_storage_account.storage.id
}

output "storage_name" {
  description = "Name of the data storage account"
  value       = azurerm_storage_account.storage.name
}

output "location" {
  description = "Location of the data storage account"
  value       = azurerm_storage_account.storage.location
}

output "primary_access_key" {
  sensitive   = true
  description = "Key used to access storage account"
  value       = azurerm_storage_account.storage.primary_access_key
}

output "secondary_access_key" {
  sensitive   = true
  description = "Key used to access storage account"
  value       = azurerm_storage_account.storage.secondary_access_key
}

output "primary_connection_string" {
  description = "The connection string for the storage account"
  value       = azurerm_storage_account.storage.primary_connection_string
}

output "container_id" {
  description = "The IDs of storage containers"
  value = tolist([
    for container in azurerm_storage_container.container : container.id
  ])
}

output "blob_id" {
  description = "The IDs of blobs"
  value = tolist([
    for blob in azurerm_storage_blob.blob : blob.id
  ])
}
output "primary_blob_url" {
  description = "Primary URL for accessing the blob storage"
  value       = azurerm_storage_account.storage.primary_blob_endpoint
}

output "share_id" {
  description = "List of file share IDs"
  value = tolist([
    for share in azurerm_storage_share.share : share.id
  ])
}
