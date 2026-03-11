# Documentation of the use of Azure Function Apps to receive and process messages from Azure Service Bus

[High level architecture](#high-level-architecture)  
[Functions folder structure](#functions-folder-structure)  
[Description of code](#description-of-code)  
[Process flow](#process-flow)  
[Function App - how it works](#function-app---how-it-works)  
[Entity configuration](#entity-configuration)  
[Wake & Drain pattern](#wake--drain-pattern)  
[Dependencies](#dependencies)  
[JSON schemas and validation](#json-schemas-and-validation)  
[Change process](#change-process)  
[Unit tests](#unit-tests)  
[Deployment](#deployment)  
[Local setup](#local-setup)  
[Terraform](#terraform)  
[Function App permissions](#function-app-permissions)  
[Issues](#issues)  
[User Accessible APIs](#user-accessible-apis)  
- [DaRT API](#dart-api)  
- [Timesheet API](#timesheet-api)  
- [Test Function API](#test-function-api)  

[Key Vault](#key-vault)

---

## High level architecture

The Azure Function App in ODW serves two main purposes:

1. **Service Bus ingestion**
   - Retrieve messages from Azure Service Bus topics
   - Validate payloads using JSON schemas from the `data-model` repository
   - Persist valid payloads to the ODW RAW storage layer

2. **HTTP APIs**
   - Expose curated ODW data through HTTP endpoints
   - Current APIs include `getDaRT`, `gettimesheets`, and `testFunction`

The current ingestion architecture supports two patterns:

- **HTTP Pull Pattern**  
  A caller such as Synapse invokes an HTTP endpoint exposed by the Function App. The function then reads from the entity's main Service Bus subscription.

- **Wake & Drain Pattern**  
  A Service Bus trigger listens on a dedicated wake subscription. When a message lands on the topic, the trigger fires and the Function App drains the real processing subscription using the Azure Service Bus SDK.

---

## Functions folder structure

Source code GitHub location:

`https://github.com/Planning-Inspectorate/ODW-Service/tree/main/functions`

~~~bash
functions/
    .funcignore
    config.yaml
    deploy.sh
    entity_registry.py
    function_app.py
    host.json
    local.settings.json
    requirements.txt
    sb_wake_drain_processor.py
    servicebus_funcs.py
    set_environment.py
    validate_messages.py
    var_funcs.py
~~~

---

## Description of code

| File | Description |
|---|---|
| `config.yaml` | Environment configuration including storage account, Service Bus namespaces, global settings and entity definitions |
| `deploy.sh` | Script used for manual deployment of the Function App |
| `entity_registry.py` | Central registry for entity definitions including schema mapping, routes, namespaces and wake subscriptions |
| `function_app.py` | Main entry point that registers all Azure Functions dynamically |
| `host.json` | Required Azure Functions host configuration file |
| `local.settings.json` | Configuration used for local development |
| `requirements.txt` | Python dependencies for the Function App |
| `sb_wake_drain_processor.py` | Implements the Wake & Drain processing logic |
| `servicebus_funcs.py` | Shared functions used by the HTTP pull ingestion pattern |
| `set_environment.py` | Loads configuration values based on environment |
| `validate_messages.py` | JSON schema validation logic |
| `var_funcs.py` | Shared runtime helpers such as Azure credentials and date/time functions |

---

## Process flow

### HTTP Pull Pattern

The HTTP Pull pattern is used when ingestion is triggered externally, for example by Synapse.

1. Synapse retrieves the Function URL and access key from Key Vault.
2. Synapse sends an HTTP GET request to the Function App.
3. The Function App reads messages from the entity's main Service Bus subscription.
4. The messages are validated against the relevant JSON schema.
5. Valid messages are written to the RAW storage layer.
6. Successfully processed messages are completed in Service Bus.
7. Downstream ODW processing reads the RAW data into later layers.

### Wake & Drain pattern

Wake & Drain separates **triggering** from **processing**.

### Topology

For an entity such as `appeal-document`, two subscriptions are used:

Main processing subscription:

~~~text
appeal-document-odw-sub
~~~

Wake subscription:

~~~text
appeal-document-odw-wake-sub
~~~

### Behaviour

1. A message is published to the Service Bus topic.
2. Service Bus copies that message to both:
   - the **main processing subscription**
   - the **wake subscription**
3. The **wake subscription** triggers the Azure Function.
4. The wake subscription message is **not used as the business payload**.
5. The wake subscription does **not** perform validation, completion, or dead-lettering of the real processing message.
6. Once triggered, the Function App starts the Wake & Drain processor.
7. The processor connects to the **main processing subscription** using the Azure Service Bus SDK.
8. The processor drains messages from the main subscription in batches.
9. Each drained message is then:
   - decoded
   - parsed as JSON
   - validated against the appropriate JSON schema
   - written to RAW storage if valid
   - completed if storage succeeds
   - explicitly dead-lettered if invalid

### Important distinction

The **wake subscription is only a trigger mechanism**.

It does **not**:

- process the real business message
- validate the payload
- write the payload to storage
- dead-letter the real payload message

All of those actions happen only when the Function App drains the **main processing subscription**.

### Why this pattern is used

This pattern allows the Function App to be triggered in near real time while still keeping full control over:

- validation
- message settlement
- explicit dead-letter reasons and descriptions
- storage write behaviour

### `sb_wake_drain_processor.py`

The Wake & Drain processor is implemented in:

~~~text
functions/sb_wake_drain_processor.py
~~~

Its main entry point is:

~~~python
def process_wake_and_drain(
    *,
    wake_msg: func.ServiceBusMessage,
    entity: EntitySpec,
    schema: dict,
    storage_account_url: str,
    storage_container: str,
    credential: Any,
    namespace: str,
    max_message_count: int,
    max_wait_time_seconds: int,
) -> None:
~~~

### Processor responsibilities

The processor:

- opens a receiver against the **main subscription**
- drains messages in batches
- validates each message
- explicitly dead-letters invalid messages on the **main subscription**
- writes valid messages to Storage
- completes valid messages only after a successful write

### Receiver creation

~~~python
receiver = sb_client.get_subscription_receiver(
    topic_name=entity.topic,
    subscription_name=entity.subscription,
    prefetch_count=100,
)
~~~

### Drain loop

~~~python
messages = receiver.receive_messages(
    max_message_count=batch_size,
    max_wait_time=max_wait_time_seconds,
)
~~~

### Explicit DLQ reasons

The processor uses explicit reasons such as:

- `BodyReadFailed`
- `InvalidJson`
- `SchemaValidationFailed`

### Important note

The wake subscription must exist in Service Bus for Wake & Drain to work.

For any new entity enabled for Wake & Drain, the wake subscription must be created unless infrastructure automation is added for it.
---

## Function App - how it works

The Function App uses the Azure Functions Python decorator model.

All ingestion functions are registered dynamically from configuration.

The main responsibilities of `function_app.py` are:

- load environment configuration
- load all JSON schemas
- register HTTP pull functions for all configured entities
- register Wake & Drain Service Bus triggers for enabled entities
- register SQL-backed API functions

The Function App is intentionally kept thin so that business logic remains in shared modules.

### `function_app.py`

The current file begins as follows:

~~~python
"""
ODW Azure Functions - Service Bus ingestion

This module is intentionally thin:
- Load env + config + schemas once
- Register all HTTP (pull) functions in a loop (as before)
- Register Wake & Drain trigger functions in a loop (kind of real time with proper DLQ reasons)
"""

from __future__ import annotations

import json
import os
from typing import Callable

import azure.functions as func

from pins_data_model import load_schemas
from var_funcs import CREDENTIAL
from set_environment import config
from servicebus_funcs import get_messages_and_validate, send_to_storage
from entity_registry import EntitySpec, all_entities
from sb_wake_drain_processor import process_wake_and_drain
~~~

It then loads runtime settings and creates the Azure Function App instance:

~~~python
try:
    _STORAGE = os.environ["MESSAGE_STORAGE_ACCOUNT"]
    _CONTAINER = os.environ["MESSAGE_STORAGE_CONTAINER"]
except Exception:
    print("Warning: Missing Environment Variables")
    _STORAGE = ""
    _CONTAINER = ""

_CREDENTIAL = CREDENTIAL
_MAX_MESSAGE_COUNT = config["global"]["max_message_count"]
_MAX_WAIT_TIME = config["global"]["max_wait_time"]
_SUCCESS_RESPONSE = config["global"]["success_response"]
_VALIDATION_ERROR = config["global"]["validation_error"]

_SCHEMAS = load_schemas.load_all_schemas()["schemas"]

_app = func.FunctionApp()

_WAKE_DRAIN_ENABLED_ENTITY_KEYS = {"appeal-document"}
~~~

### Dynamic HTTP registration

Each entity in `config.yaml` gets an HTTP GET function dynamically.

The HTTP handler factory is:

~~~python
def _make_http_pull_handler(entity: EntitySpec) -> Callable[[func.HttpRequest], func.HttpResponse]:
    """
    Builds an HTTP GET handler for an entity
    """
    def _handler(req: func.HttpRequest) -> func.HttpResponse:
        schema = _SCHEMAS[entity.schema_filename]
        topic = entity.topic
        subscription = entity.subscription

        namespace = os.environ.get(entity.http_namespace_env_var, "")

        try:
            data = get_messages_and_validate(
                namespace=namespace,
                credential=_CREDENTIAL,
                topic=topic,
                subscription=subscription,
                max_message_count=_MAX_MESSAGE_COUNT,
                max_wait_time=_MAX_WAIT_TIME,
                schema=schema,
            )

            message_count = send_to_storage(
                account_url=_STORAGE,
                credential=_CREDENTIAL,
                container=_CONTAINER,
                entity=entity.storage_entity,
                data=data,
            )

            response = json.dumps({
                "message": f"{_SUCCESS_RESPONSE} - {message_count} messages sent to storage",
                "count": message_count
            })

            return func.HttpResponse(response, status_code=200)

        except Exception as e:
            return (
                func.HttpResponse(f"Validation error: {str(e)}", status_code=500)
                if f"{_VALIDATION_ERROR}" in str(e)
                else func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
            )

    return _handler
~~~

### Dynamic Wake & Drain registration

For entities enabled in `_WAKE_DRAIN_ENABLED_ENTITY_KEYS`, the Function App also registers a Service Bus trigger:

~~~python
def _make_wake_drain_trigger_handler(entity: EntitySpec) -> Callable[[func.ServiceBusMessage], None]:
    """
    Builds a Service Bus trigger handler for an entity
    The trigger listens to entity.trigger_subscription (the wake subscription)
    The draining is performed against entity.subscription (the real subscription)
    """
    def _handler(msg: func.ServiceBusMessage) -> None:
        schema = _SCHEMAS[entity.schema_filename]

        namespace = os.environ.get(entity.http_namespace_env_var, "")

        process_wake_and_drain(
            wake_msg=msg,
            entity=entity,
            schema=schema,
            storage_account_url=_STORAGE,
            storage_container=_CONTAINER,
            credential=_CREDENTIAL,
            namespace=namespace,
            max_message_count=_MAX_MESSAGE_COUNT,
            max_wait_time_seconds=_MAX_WAIT_TIME,
        )

    return _handler
~~~

### Registration loop

Functions are registered using the central entity list:

~~~python
for entity in all_entities():
    http_fn_name = entity.route
    http_route = entity.route

    _app.function_name(http_fn_name)(
        _app.route(route=http_route, methods=["get"], auth_level=func.AuthLevel.FUNCTION)(
            _make_http_pull_handler(entity)
        )
    )

    if entity.key in _WAKE_DRAIN_ENABLED_ENTITY_KEYS:
        if not entity.wake_subscription:
            raise ValueError(
                f"Wake&Drain enabled for {entity.key} but no wake_subscription is configured"
            )

        sb_fn_name = f"{entity.route}_wake_drain"

        _app.function_name(sb_fn_name)(
            _app.service_bus_topic_trigger(
                arg_name="msg",
                topic_name=entity.topic,
                subscription_name=entity.trigger_subscription,
                connection=entity.sb_connection,
            )(
                _make_wake_drain_trigger_handler(entity)
            )
        )
~~~

In addition to ingestion, `function_app.py` also contains the SQL-backed APIs:

- `getDaRT`
- `gettimesheets`
- `testFunction`

---

## Entity configuration

Entities are defined centrally in `config.yaml`.

Current structure:

~~~yaml
dev:
  storage_account: "https://pinsstodwdevuks9h80mb.blob.core.windows.net"
  storage_container: "odw-raw/ServiceBus"
  servicebus_namespace_odt: "pins-sb-back-office-dev-ukw-001.servicebus.windows.net"
  servicebus_namespace_odw: "pins-sb-odw-dev-uks-b9rt9m.servicebus.windows.net"

preprod:
  storage_account: "https://pinsstodwtestukswic3ai.blob.core.windows.net"
  storage_container: "odw-raw/ServiceBus"
  servicebus_namespace_odt: "pins-sb-back-office-test-ukw-001.servicebus.windows.net"
  servicebus_namespace_odw: "pins-sb-odw-test-uks-hk2zun.servicebus.windows.net"

prod:
  storage_account: "https://pinsstodwprodukson83nw.blob.core.windows.net"
  storage_container: "odw-raw/ServiceBus"
  servicebus_namespace_odt: "pins-sb-back-office-prod-ukw-001.servicebus.windows.net"
  servicebus_namespace_odw: "pins-sb-odw-prod-uks-mwzecc.servicebus.windows.net"

global:
  odt-schemas-repo: "https://github.com/Planning-Inspectorate/data-model/tree/main/schemas"
  max_message_count: 100000
  max_wait_time: 5
  success_response: "Function executed successfully"
  validation_error: "Failed validating"

  entities:
    folder:
      topic: "folder"
      subscription: "odw-folder-sub"

    nsip-document:
      topic: "nsip-document"
      subscription: "odw-nsip-document-sub"

    nsip-exam-timetable:
      topic: "nsip-exam-timetable"
      subscription: "odw-nsip-exam-timetable-sub"

    nsip-project:
      topic: "nsip-project"
      subscription: "odw-nsip-project-sub"

    nsip-project-update:
      topic: "nsip-project-update"
      subscription: "odw-nsip-project-update-sub"

    nsip-representation:
      topic: "nsip-representation"
      subscription: "odw-nsip-representation-sub"

    nsip-s51-advice:
      topic: "nsip-s51-advice"
      subscription: "odw-nsip-s51-advice-sub"

    nsip-subscription:
      topic: "nsip-subscription"
      subscription: "odw-nsip-subscription-sub"

    service-user:
      topic: "service-user"
      subscription: "odw-service-user-sub"

    appeal-document:
      topic: "appeal-document"
      subscription: "appeal-document-odw-sub"

    appeal-has:
      topic: "appeal-has"
      subscription: "appeal-has-odw-sub"

    appeal-event:
      topic: "appeal-event"
      subscription: "appeal-event-odw-sub"

    appeal-event-estimate:
      topic: "appeal-event-estimate"
      subscription: "appeal-event-estimate-odw-sub"

    appeal-service-user:
      topic: "appeal-service-user"
      subscription: "appeal-service-user-odw-sub"

    appeal-s78:
      topic: "appeal-s78"
      subscription: "appeal-s78-odw-sub"

    appeal-representation:
      topic: "appeal-representation"
      subscription: "appeal-representation-odw-sub"
~~~

### `entity_registry.py`

This file builds an `EntitySpec` object for each configured entity.

~~~python
@dataclass(frozen=True)
class EntitySpec:
    key: str
    topic: str
    subscription: str
    schema_filename: str
    sb_connection: str
    http_namespace_env_var: str
    storage_entity_override: Optional[str] = None
    http_route: Optional[str] = None
    wake_subscription: Optional[str] = None

    @property
    def storage_entity(self) -> str:
        return self.storage_entity_override or self.topic

    @property
    def route(self) -> str:
        return self.http_route or self.key.replace("-", "")

    @property
    def trigger_subscription(self) -> str:
        return self.wake_subscription or self.subscription
~~~

The registry is responsible for:

- schema filename overrides
- route naming
- storage folder overrides
- namespace rules
- wake subscription definitions

Current schema overrides:

~~~python
SCHEMA_OVERRIDES = {
    "appeal-service-user": "service-user.schema.json",
    "nsip-s51-advice": "s51-advice.schema.json",
}
~~~

Current Wake & Drain pilot configuration:

~~~python
if entity_key == "appeal-document":
    wake_subscription = "appeal-document-odw-wake-sub"
~~~

---

## Wake & Drain pattern

Wake & Drain separates **triggering** from **processing**.

### Topology

For an entity such as `appeal-document`, two subscriptions are used:

Main processing subscription:

~~~text
appeal-document-odw-sub
~~~

Wake subscription:

~~~text
appeal-document-odw-wake-sub
~~~

### Behaviour

1. A message is published to the topic.
2. Service Bus copies the message to both subscriptions.
3. The wake subscription triggers the Function App.
4. The Function App drains the main subscription using the Service Bus SDK.

### `sb_wake_drain_processor.py`

Current header:

~~~python
"""
Wake + Drain Processor

Pattern:
- A lightweight wake subscription triggers the Function when messages arrive
- The Function then drains the REAL subscription using the azure-servicebus SDK
- On validation failure we explicitly dead letter messages on the REAL subscription
  so that DeadLetterReason and DeadLetterErrorDescription are populated in Service Bus

This avoids not having a hand on the errors if staying fully with ServiceBusTrigger
"""
~~~

Main entry point:

~~~python
def process_wake_and_drain(
    *,
    wake_msg: func.ServiceBusMessage,
    entity: EntitySpec,
    schema: dict,
    storage_account_url: str,
    storage_container: str,
    credential: Any,
    namespace: str,
    max_message_count: int,
    max_wait_time_seconds: int,
) -> None:
~~~

### Main responsibilities

The processor:

- opens a receiver against the **main** subscription
- drains messages in batches
- validates each message
- explicitly dead-letters invalid messages
- writes valid messages to Storage
- completes valid messages only after a successful write

### Receiver creation

~~~python
receiver = sb_client.get_subscription_receiver(
    topic_name=entity.topic,
    subscription_name=entity.subscription,
    prefetch_count=100,
)
~~~

### Drain loop

~~~python
messages = receiver.receive_messages(
    max_message_count=batch_size,
    max_wait_time=max_wait_time_seconds,
)
~~~

### Explicit DLQ reasons

The processor uses explicit reasons such as:

- `BodyReadFailed`
- `InvalidJson`
- `SchemaValidationFailed`

### Important note

The wake subscription must exist in Service Bus for Wake & Drain to work.

For any new entity enabled for Wake & Drain, the wake subscription must be created unless infrastructure automation is added for it.

---

## Dependencies

Current `requirements.txt`:

~~~text
azure-identity==1.15.0
azure-functions==1.20.0
azure-servicebus==7.13.0
azure-storage-blob==12.19.0
azure-mgmt-web==7.2.0
azure-keyvault==4.2.0
PyYAML==6.0.1
jsonschema==4.20.0
iso8601==2.1.0
aiohttp==3.13.3
pytest==7.4.0
pytest-asyncio==0.23.3
git+https://github.com/Planning-Inspectorate/data-model@main#egg=pins_data_model
~~~

Schemas are loaded from the central `data-model` repository:

~~~python
from pins_data_model import load_schemas
~~~

---

## JSON schemas and validation

The `data-model` repository is the source of truth for message schemas.

Links:

- [JSON schemas](https://github.com/Planning-Inspectorate/data-model/tree/main/schemas)
- [Pydantic models](https://github.com/Planning-Inspectorate/data-model/tree/main/pins_data_model/models)

### Current validation implementation

`validate_messages.py` currently contains:

~~~python
"""
Module containing a validate function to be called to run validation
of a list of sevricebus messages.

The validation uses jsonschema with a format checker function to 
check that the date-time is compliant with ISO8601.
"""

from jsonschema import validate, FormatChecker, ValidationError
from iso8601 import parse_date, ParseError


def is_iso8601_date_time(instance) -> bool:
    """
    Function to check if a date matches ISO-8601 format
    """

    if instance is None:
        return True
    try:
        parse_date(instance)
        return True
    except ParseError:
        return False


def validate_data(message, schema: dict) -> bool:
    """
    Function to validate a list of servicebus messages.
    Validation includes a format check against ISO-8601.
    """

    format_checker = FormatChecker()
    format_checker.checks("date-time")(is_iso8601_date_time)

    errors = []
    try:
        validate(instance=message, schema=schema, format_checker=format_checker)
    except ValidationError as e:
        error_path = "/".join([str(p) for p in e.path]) or "<root>"
        errors = [f"{error_path}: {e.message}"]
    return errors
~~~

### Notes

- Validation is performed message by message
- An empty list means validation succeeded
- A non-empty list contains validation errors

---

## Change process

### Add a new entity

1. Add the entity to `config.yaml`
2. Confirm the schema mapping in `entity_registry.py`
3. Add any special route or storage overrides if required
4. Deploy the Function App
5. Test the generated route

### Enable Wake & Drain

1. Create the wake subscription in Service Bus
2. Add the wake subscription mapping in `entity_registry.py`
3. Add the entity to `_WAKE_DRAIN_ENABLED_ENTITY_KEYS` in `function_app.py`
4. Deploy the Function App
5. Validate trigger, drain, Storage write, and DLQ behaviour

### Remove an entity

1. Remove the entity from `config.yaml`
2. Remove any special handling from `entity_registry.py`
3. Remove the entity from `_WAKE_DRAIN_ENABLED_ENTITY_KEYS` if present
4. Deploy the Function App

---

## Unit tests

Tests are located under:

~~~text
functions/tests
~~~

Run tests with:

~~~bash
python -m pytest -v
~~~

These tests primarily validate HTTP endpoints.

Wake & Drain behaviour should also be validated in lower environments because it depends on:

- Service Bus subscriptions
- trigger registration
- namespace connectivity
- DLQ behaviour
- Azure Storage writes

---

## Deployment

Deployment is handled through Azure DevOps using the **Deploy ODW Azure Functions** pipeline.

This pipeline is used for both:

- deploying a feature or working branch to a lower environment for testing
- deploying `main` as the release version

### Pipeline

Azure DevOps pipeline:

`Deploy ODW Azure Functions`

The pipeline allows the user to select:

- the branch, commit or tag to run
- the target environment

Supported environments are:

- `dev`
- `test`
- `prod`

### Typical deployment usage

#### Deploy a branch to Dev or Test

A feature branch can be deployed manually to a non-production environment in order to test Function App changes before merging.

Typical workflow:

1. Open the **Deploy ODW Azure Functions** pipeline in Azure DevOps
2. Click **Run pipeline**
3. Select the branch to deploy
4. Choose the target environment:
   - `dev` for development testing
   - `test` for validation in the test environment
5. Run the pipeline

This allows changes such as new entity configuration, route changes, Wake & Drain rollout, or API updates to be tested before release.

#### Release `main` to Prod

Production deployments should use the `main` branch.

Typical workflow:

1. Open the **Deploy ODW Azure Functions** pipeline
2. Click **Run pipeline**
3. Select `main`
4. Choose `prod`
5. Run the pipeline

This is the release path for approved Function App changes.

## Local setup

Recommended local setup steps:

1. Install Azure Functions Core Tools
2. Install Python dependencies
3. Open the `functions` folder in VS Code
4. Start the Function App locally

Example:

~~~bash
func start
~~~

### Important note

When running locally, the code can still connect to Azure Service Bus and Azure Storage unless mocked or redirected.  
Your local account therefore still needs the appropriate access and configuration.

---

## Terraform

The Function App infrastructure is provisioned using Terraform.

Repository location:

~~~text
infrastructure/modules/function-app
~~~

Terraform provisions the Function App and associated infrastructure, but successful runtime execution also depends on:

- configuration
- RBAC
- networking
- namespace connectivity

---

## Function App permissions

The Function App uses a **system-assigned managed identity**.

### Storage

Required role:

~~~text
Storage Blob Data Contributor
~~~

Used to write messages to the RAW layer.

### Service Bus

Required role:

~~~text
Azure Service Bus Data Receiver
~~~

Used to read from Service Bus subscriptions.

### SQL

Required for the SQL-backed APIs such as:

- `getDaRT`
- `gettimesheets`
- `testFunction`

### Networking

If Service Bus namespaces use:

- private endpoints
- selected networks
- IP filtering

the Function App must also have network access to the namespace.

Without this, triggers may fail even if RBAC is correct.

---

## Issues

### Deployment method

Using different deployment methods against the same Function App can disable functions in the Azure portal.

Use a consistent deployment mechanism.

### Networking

In secured environments, Service Bus access can fail with errors such as:

- `Put token failed`
- `401`
- `Ip has been prevented to connect to the endpoint`

These indicate a network access problem rather than only an RBAC problem.

### Wake & Drain scope

Wake & Drain is currently enabled only for selected entities, not all entities.

---

## User Accessible APIs

The Function App also exposes business-facing APIs backed by SQL input bindings.

---

## DaRT API

Returns records from the curated `dart_api` table.

Endpoint:

~~~text
/api/getDaRT
~~~

Current implementation:

~~~python
@_app.function_name(name="getDaRT")
@_app.route(route="getDaRT", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(arg_name="dart",
                command_text="""
                SELECT *
                FROM odw_curated_db.dbo.dart_api
                WHERE UPPER([applicationReference]) = UPPER(@applicationReference) 
                OR UPPER([caseReference]) = UPPER(@caseReference)
                """,
                command_type="Text",
                parameters="@caseReference={caseReference},@applicationReference={applicationReference}",
                connection_string_setting="SqlConnectionString"
                )
def getDaRT(req: func.HttpRequest, dart: func.SqlRowList) -> func.HttpResponse:
    try:
        rows = []
        for r in dart:
            row = json.loads(r.to_json())
            for key, value in row.items():
                if isinstance(value, str):
                    try:
                        parsed_value = json.loads(value)
                        row[key] = parsed_value
                    except json.JSONDecodeError as e:
                        row[key] = value
            rows.append(row)
        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
~~~

Example URL:

~~~text
https://<FUNCTION_APP_URL>/api/getDaRT?code=<ACCESS_TOKEN>&applicationReference=<APPLICATION_REFERENCE>&caseReference=<CASE_REFERENCE>
~~~

---

## Timesheet API

Performs wildcard search across selected curated `appeal_has` fields.

Endpoint:

~~~text
/api/gettimesheets
~~~

Current implementation:

~~~python
@_app.function_name(name="gettimesheets")
@_app.route(route="gettimesheets", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(arg_name="timesheet",
                command_text="SELECT [caseReference], [applicationReference], [siteAddressLine1], [siteAddressLine2], [siteAddressTown], [siteAddressCounty], [siteAddressPostcode] FROM [odw_curated_db].[dbo].[appeal_has] WHERE UPPER([caseReference]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([applicationReference]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressLine1]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressLine2]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressTown]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressCounty]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37)) OR UPPER([siteAddressPostcode]) LIKE Concat(Char(37), UPPER(@searchCriteria), Char(37))",
                command_type="Text",
                parameters="@searchCriteria={searchCriteria}",
                connection_string_setting="SqlConnectionString")
def gettimesheets(req: func.HttpRequest, timesheet: func.SqlRowList) -> func.HttpResponse:
    """
    We need to use Char(37) to escape the %
    https://stackoverflow.com/questions/71914897/how-do-i-use-sql-like-value-operator-with-azure-functions-sql-binding
    """
    try:
        rows = list(map(lambda r: json.loads(r.to_json()), timesheet))
        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return (
            func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
        )
~~~

Example URL:

~~~text
https://<FUNCTION_APP_URL>/api/gettimesheets?code=<ACCESS_TOKEN>&searchCriteria=<SEARCH_CRITERIA>
~~~

Searches across:

- `caseReference`
- `applicationReference`
- `siteAddressLine1`
- `siteAddressLine2`
- `siteAddressTown`
- `siteAddressCounty`
- `siteAddressPostcode`

---

## Test Function API

Diagnostic endpoint returning rows from the logging database.

Endpoint:

~~~text
/api/testFunction
~~~

Current implementation:

~~~python
@_app.function_name(name="testFunction")
@_app.route(route="testFunction", methods=["get"], auth_level=func.AuthLevel.FUNCTION)
@_app.sql_input(
    arg_name="logs",
    command_text="""
    SELECT TOP (1000) [file_ID],
                    [ingested_datetime],
                    [ingested_by_process_name],
                    [input_file],
                    [modified_datetime],
                    [modified_by_process_name],
                    [entity_name],
                    [rows_raw],
                    [rows_new]
    FROM logging.dbo.tables_logs
    """,
    command_type="Text",
    connection_string_setting="SqlConnectionString"
)
def test_function(req: func.HttpRequest, logs: func.SqlRowList) -> func.HttpResponse:
    try:
        rows = []
        for r in logs:
            rows.append(json.loads(r.to_json()))

        return func.HttpResponse(
            json.dumps(rows),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        return func.HttpResponse(f"Unknown error: {str(e)}", status_code=500)
~~~

---

## Key Vault

Function URLs and access keys are stored in Azure Key Vault.

Helper script:

~~~text
functions/helper/getfunctionurlsandsetkeyvaultsecrets.py
~~~

This script retrieves function URLs and updates Key Vault secrets.

Running it requires:

- Azure CLI installed
- `az login`
- the appropriate Azure and Key Vault permissions