
import requests
import sys
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)

def send_telemetry_to_app_insights(params):
    try:
        key = mssparkutils.credentials.getSecretWithLS("ls_kv", "application-insights-connection-string")
        instrumentation_key = key.split("InstrumentationKey=")[-1].split(";")[0]
    except Exception as e:
        logging.error("Failed to get instrumentation key: %s", e)
        return

    endpoint = "https://uksouth-1.in.applicationinsights.azure.com/v2/track"
    headers = {
        "Content-Type": "application/json"
    }

    body = {
        "Stage": params.get("Stage"),
        "PipelineName": params.get("PipelineName"),
        "PipelineRunID": params.get("PipelineRunID"),
        "StartTime": params.get("StartTime"),
        "EndTime": params.get("EndTime"),
        "Inserts": str(params.get("Inserts", 0)),
        "Updates": str(params.get("Updates", 0)),
        "Deletes": str(params.get("Deletes", 0)),
        "ErrorMessage": params.get("ErrorMessage"),
        "StatusMessage": params.get("StatusMessage"),
        "PipelineTriggerID": params.get("PipelineTriggerID"),
        "PipelineTriggerName": params.get("PipelineTriggerName"),
        "PipelineTriggerType": params.get("PipelineTriggerType"),
        "PipelineTriggeredbyPipelineName": params.get("PipelineTriggeredbyPipelineName"),
        "PipelineTriggeredbyPipelineRunID": params.get("PipelineTriggeredbyPipelineRunID"),
        "PipelineExecutionTimeInSec": params.get("PipelineExecutionTimeInSec"),
        "ActivityType": params.get("ActivityType"),
        "StatusCode": params.get("StatusCode"),
        "DurationSeconds": params.get("DurationSeconds")
    }

    payload = {
        "name": "Microsoft.ApplicationInsights.Event",
        "time": datetime.utcnow().isoformat() + "Z",
        "iKey": instrumentation_key,
        "data": {
            "baseType": "EventData",
            "baseData": {
                "name": params.get("AppInsCustomEventName", "ODW_Master_Pipeline_Logs"),
                "properties": body
            }
        }
    }

    try:
        response = requests.post(endpoint, json=payload, headers=headers)
        logging.info("Telemetry sent: %s", response.status_code)
    except Exception as e:
        logging.error("Failed to send telemetry: %s", e)

def log_telemetry_and_exit(
    stage,
    start_exec_time,
    end_exec_time,
    error_message,
    table_name,
    insert_count,
    update_count,
    delete_count,
    PipelineName,
    PipelineRunID,
    PipelineTriggerID,
    PipelineTriggerName,
    PipelineTriggerType,
    PipelineTriggeredbyPipelineName,
    PipelineTriggeredbyPipelineRunID,
    activity_type,
    duration_seconds,
    status_message,
    status_code
):
    params = {
        "Stage": stage,
        "PipelineName": PipelineName,
        "PipelineRunID": PipelineRunID,
        "StartTime": start_exec_time.isoformat(),
        "EndTime": end_exec_time.isoformat(),
        "Inserts": insert_count,
        "Updates": update_count,
        "Deletes": delete_count,
        "ErrorMessage": error_message,
        "StatusMessage": status_message,
        "PipelineTriggerID": PipelineTriggerID,
        "PipelineTriggerName": PipelineTriggerName,
        "PipelineTriggerType": PipelineTriggerType,
        "PipelineTriggeredbyPipelineName": PipelineTriggeredbyPipelineName,
        "PipelineTriggeredbyPipelineRunID": PipelineTriggeredbyPipelineRunID,
        "PipelineExecutionTimeInSec": duration_seconds,
        "ActivityType": activity_type,
        "DurationSeconds": duration_seconds,
        "StatusCode": status_code,
        "AppInsCustomEventName": "ODW_Master_Pipeline_Logs"
    }

    send_telemetry_to_app_insights(params)

    if error_message:
        logging.error("Notebook Failed for load %s: %s", table_name, error_message)
        sys.exit(1)
