resource "azurerm_logic_app_workflow" "zendesk_updated" {
  count = var.logic_app_enabled ? 1 : 0

  name                = "pins-la-zendesk-updated-${local.resource_suffix}"
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
        "connectionId" : jsondecode(azurerm_resource_group_template_deployment.zendesk_custom_api_template[count.index].output_content).connectionId.value,
        "connectionName" : azurerm_resource_group_template_deployment.zendesk_custom_api_template[count.index].name,
        "id" : azapi_resource.zendesk_custom_api[count.index].id
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

resource "azurerm_logic_app_trigger_custom" "zendesk_updated_trigger" {
  count = var.logic_app_enabled ? 1 : 0

  logic_app_id = azurerm_logic_app_workflow.zendesk_updated[count.index].id
  name         = "Triggers_when_a_new_ticket_is_updated"

  body = jsonencode({
    "evaluatedRecurrence" : {
      "frequency" : "Hour",
      "interval" : 1
    },
    "inputs" : {
      "host" : {
        "connection" : {
          "name" : "@parameters('$connections')['zendesk-custom-api']['connectionId']"
        }
      },
      "method" : "get",
      "path" : "/api/v2/search.json",
      "queries" : {
        "query" : "?query=type:ticket updated_at>1hour order_by:updated_at sort:desc"
      }
    },
    "recurrence" : {
      "frequency" : "Hour",
      "interval" : 1
    },
    "splitOn" : "@triggerBody()?['value']",
    "type" : "ApiConnection"
  })
}

resource "azurerm_logic_app_action_custom" "zendesk_updated_action" {
  count = var.logic_app_enabled ? 1 : 0

  logic_app_id = azurerm_logic_app_workflow.zendesk_updated[count.index].id
  name         = "Send_message"

  body = jsonencode({
    "inputs" : {
      "body" : {
        "ContentData" : "@{base64(triggerOutputs())}",
        "Label" : "Updated",
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
