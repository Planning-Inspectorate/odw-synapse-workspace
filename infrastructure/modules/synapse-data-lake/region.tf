module "azure_region" {
  #checkov:skip=CKV_TF_1: Ensure Terraform module sources use a commit hash (checkov v3)
  source  = "claranet/regions/azurerm"
  version = "5.1.0"

  azure_region = var.location
}
