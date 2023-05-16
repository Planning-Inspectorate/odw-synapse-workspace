data "azuread_group" "groups" {
  count        = length(local.group_names)
  display_name = local.group_names[count.index]
}

data "azuread_user" "users" {
  count               = length(local.user_principal_names)
  user_principal_name = local.user_principal_names[count.index]
}

data "azuread_service_principal" "service_principals" {
  count        = length(local.service_principal_names)
  display_name = local.service_principal_names[count.index]
}
