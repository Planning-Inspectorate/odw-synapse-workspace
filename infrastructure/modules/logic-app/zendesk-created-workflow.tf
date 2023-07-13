resource "azurerm_logic_app_workflow" "zendesk_created" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "pins-la-zendesk-created-${local.resource_suffix}"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = local.tags
  workflow_parameters = {
    "$connections" = jsonencode({
      "defaultValue" : {},
      "type" : "Object"
    })
  }

  parameters = {
    "$connections" = jsonencode({
      "zendesk-custom-api" : {
        "connectionId" : azurerm_resource_group_template_deployment.zendesk_custom_api_template[count.index].id,
        "connectionName" : azurerm_resource_group_template_deployment.zendesk_custom_api_template[count.index].name,
        "id" : azurerm_resource_group_template_deployment.zendesk_custom_api_template[count.index].id
      },
      "servicebus" : {
        "connectionId" : azurerm_api_connection.service_bus_api_connection[count.index].id,
        "connectionName" : azurerm_api_connection.service_bus_api_connection[count.index].name,
        "id" : azurerm_api_connection.service_bus_api_connection[count.index].managed_api_id
      }
    })
  }
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_logic_app_trigger_custom" "zendesk_created_trigger" {
  count = var.logic_app_enabled ? 1 : 0

  logic_app_id = azurerm_logic_app_workflow.zendesk_created[count.index].id
  name         = "When_an_item_is_created"

  body = jsonencode({
    "evaluatedRecurrence" : {
      "frequency" : "Minute",
      "interval" : 1
    },
    "inputs" : {
      "host" : {
        "connection" : {
          "name" : "@parameters('$connections')['zendesk']['connectionId']"
        }
      },
      "method" : "get",
      "path" : "/datasets/default/tables/@{encodeURIComponent(encodeURIComponent('tickets'))}/onnewitems"
    },
    "recurrence" : {
      "frequency" : "Minute",
      "interval" : 1
    },
    "splitOn" : "@triggerBody()?['value']",
    "type" : "ApiConnection"
  })
}

resource "azurerm_logic_app_action_custom" "zendesk_created_action" {
  count = var.logic_app_enabled ? 1 : 0

  logic_app_id = azurerm_logic_app_workflow.zendesk_created[count.index].id
  name         = "Send_message"

  body = jsonencode({
    "inputs" : {
      "body" : {
        "ContentData" : "@{base64(triggerOutputs())}",
        "Label" : "Created",
        "MessageId" : "@{guid()}",
        "SessionId" : "@{guid()}"
      },
      "host" : {
        "connection" : {
          "name" : "@parameters('$connections')['servicebus']['connectionId']"
        }
      },
      "method" : "post",
      "path" : "/@{encodeURIComponent(encodeURIComponent('zendesk'))}/messages",
      "queries" : {
        "systemProperties" : "None"
      }
    },
    "runAfter" : {},
    "type" : "ApiConnection"
  })
}
