resource "azapi_resource" "zendesk_custom_api" {
  count = var.logic_app_enabled ? 1 : 0

  type      = "Microsoft.Web/customApis@2016-06-01"
  name      = "zendesk-custom-api"
  location  = var.location
  parent_id = var.resource_group_id
  tags      = local.tags

  body = jsonencode({
    "properties" : {
      "connectionParameters" : {
        "username" : {
          "type" : "securestring",
          "uiDefinition" : {
            "displayName" : "username",
            "description" : "The username for this api",
            "tooltip" : "Provide the username",
            "constraints" : {
              "tabIndex" : 2,
              "clearText" : true,
              "required" : "true"
            }
          }
        },
        "password" : {
          "type" : "securestring",
          "uiDefinition" : {
            "displayName" : "password",
            "description" : "The password for this api",
            "tooltip" : "Provide the password",
            "constraints" : {
              "tabIndex" : 3,
              "clearText" : false,
              "required" : "true"
            }
          }
        }
      },
      "capabilities" : [],
      "description" : "Custom API for Zendesk",
      "displayName" : "zemdesk-custom-api",
      "iconUri" : "https://content.powerapps.com/resource/makerx/static/media/default-connection-icon.00d06b6e.svg",
      "swagger" : file("${path.module}/zendesk-swagger.json"),
      "apiType" : "Rest",
      "wsdlDefinition" : {}
    }
  })
}
