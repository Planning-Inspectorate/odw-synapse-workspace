# Anonymisation package — logical model

This package provides column-level anonymisation for PySpark DataFrames driven by Azure Purview classifications. It maps Purview classification types to deterministic masking/transformation strategies and applies them to a DataFrame, with optional configuration to scope which classifications are acted upon.

## High-level flow

1. Input DataFrame (Spark) and a list of column classifications (from Purview or supplied explicitly).
2. Normalise DataFrame column names to match against classification results.
3. For each classified column, pick the first matching strategy and transform the column.
4. When using Purview, the engine can fetch classifications by asset and report how many rows changed.
5. Emit structured, PII-safe logs throughout.

Core modules:
- `base.py` — strategy interface, helper UDFs, and default strategies
- `engine.py` — orchestration, Purview integration, logging
- `config.py` — `AnonymisationConfig` and YAML loader
- `__init__.py` — public API re-exports

### Module reference

- base.py
  - Types: `BaseStrategy` (interface), concrete strategies `NINumberStrategy`, `EmailMaskStrategy`, `NameMaskStrategy`, `BirthDateStrategy`, `AgeStrategy`, `SalaryStrategy`
  - Helpers on BaseStrategy: `BaseStrategy.seed_col(df)`, `BaseStrategy.mask_keep_first_last(col)`, `BaseStrategy.random_int_from_seed(seed, min, max)`, `BaseStrategy.random_date_from_seed(seed, start, end)`
  - Factory: `default_strategies()` returns the built-in strategy list in precedence order
  - Public class-level UDFs on strategies — `NameMaskStrategy.mask_fullname_initial_lastletter_udf`, `NameMaskStrategy.mask_name_first_only_udf`, `EmailMaskStrategy.mask_email_preserve_domain_udf`, `NINumberStrategy.generate_random_ni_number_udf`

- engine.py
  - Class: `AnonymisationEngine(strategies=None, config=None, run_id=None)`
    - `apply(df, columns_with_classifications, classification_allowlist=None) -> DataFrame`
    - `apply_from_purview(...) -> DataFrame` supports:
      - Explicit form: `purview_name, tenant_id, client_id, client_secret, asset_type_name, asset_qualified_name, api_version?, classification_allowlist?`
      - Simplified form: `source_folder` in {`ServiceBus`, `Horizon`, `entraid`} plus `entity_name` or `file_name` (builds a Purview ADLS Gen2 resource-set qualified name)
  - Purview helpers: `fetch_purview_classifications_by_qualified_name(...)` resolves entity and extracts column classifications
  - Matching: column names are normalised (trim/lower/remove non-alnum, fix common typos) before matching
  - Observability: emits structured, PII-safe logs: `apply.start`, `apply.summary`, `purview.resolve.*`, `apply_from_purview.columns_selected`, `apply_from_purview.summary`
  - Credentials & defaults:
    - Client secret resolution via env/KeyVault (see “Credentials and defaults” below)
    - Storage host resolution for simplified mode via env/`mssparkutils` (see “Credentials and defaults”)
    - `run_id` can be supplied or read from `ODW_RUN_ID` for correlation

- config.py
  - Dataclass: `AnonymisationConfig(classification_allowlist: Optional[Set[str]])`
  - Loader: `load_config(path=None, text=None) -> AnonymisationConfig` (YAML keys: `classification_allowlist`)

## Core concepts

### Strategies
A strategy encapsulates how to anonymise one column when certain classifications are present.

- Interface: `BaseStrategy.apply(df, column, seed, context) -> DataFrame`
- Trigger: `BaseStrategy.classification_names` — set of Purview classification type names
- Precedence: the first strategy (by order in the `strategies` list) whose `classification_names` intersects the column’s classifications is applied
- Context passed to `apply`: `context["classifications"]` is the set of matched classification names; `context["is_lm"]` is `True` when the target column name contains "line manager" (available for custom strategies)

Default strategies and their behaviour:

- NI number (`NINumberStrategy`)
  - Triggers: `{"NI Number"}`
  - Transform: generate a random NI number in the format `AA000000A` (two letters, six digits, final letter A–D)
  - Determinism: non-deterministic per row and per run (uses Python RNG)

- Email (`EmailMaskStrategy`)
  - Triggers: `{"MICROSOFT.PERSONAL.EMAIL", "Email Address", "Email Address Column Name"}`
  - Transform: mask email local part, keep first and last char; preserve domain exactly
    - Example: `john.doe@pins.com` → `j******e@pins.com`
    - For non-email strings, mask the entire string similarly
  - Determinism: deterministic given the input value

- Name (`NameMaskStrategy`)
  - Triggers: `{"MICROSOFT.PERSONAL.NAME", "First Name", "Last Name", "Names Column Name"}`
  - Transform: if the value looks like a full name (contains whitespace), keep:
    - first letter of the first name and last letter of the last name; mask the rest
    - otherwise (single token), keep only first letter and mask the rest
    - Fallback heuristic (when no classification): if column name contains `name` but not `first`/`last`, treat as full name; otherwise keep only first and last letters
  - Determinism: deterministic given the input value

- Birth date (`BirthDateStrategy`)
  - Triggers: `{"Birth Date", "Date of Birth"}`
  - Transform: replace with a pseudo-random date between `1955-01-01` (inclusive) and `2005-12-31` (exclusive) derived from the seed column
  - Determinism: deterministic per row when a seed column is present

- Age (`AgeStrategy`)
  - Triggers: `{"Person's Age", "Employee Age"}`
  - Transform: replace with a pseudo-random integer in `[18, 70]` derived from the seed
  - Determinism: deterministic per row when a seed column is present

- Salary (`SalaryStrategy`)
  - Triggers: `{"Annual Salary"}`
  - Transform: replace with a pseudo-random integer in `[20000, 100000]` derived from the seed
  - Determinism: deterministic per row when a seed column is present

### Seed column and determinism
Some strategies derive pseudo-random values by hashing a “seed column” so the result is stable for a given person/row.

- Seed column candidates (first present is used): `"Staff Number"`, `"PersNo"`, `"PersNo."`, `"Personnel Number"`, `"Employee ID"`, `"EmployeeID"`
- Fallback: if none are present, a constant seed is used, which makes seeded outputs constant across the entire DataFrame
- Note: NI numbers are deliberately non-deterministic (generated fresh) and do not use the seed

### Column name normalisation
To align classification results with DataFrame columns despite naming differences, names are normalised by:
- trimming and lowercasing
- removing non-alphanumeric characters (so `first_name` ≈ `FirstName` ≈ `first name`)
- fixing common typos (e.g. `adress` → `address`)

### Configuration (`AnonymisationConfig`)
Optional configuration can restrict which classifications are eligible for anonymisation:

- `classification_allowlist`: set of classification type names to allow; others are ignored
- Load from YAML via `load_config(path=..., text=...)`

Example YAML:
```yaml
classification_allowlist:
  - MICROSOFT.PERSONAL.NAME
  - MICROSOFT.PERSONAL.EMAIL
  - NI Number
```

## Engine behaviour

### Direct application (`apply`)
- Inputs: DataFrame and a list of `{column_name, classifications}` dicts
- For each entry:
  - The engine normalises `column_name` and finds the actual DataFrame column
  - It intersects the column’s classifications with the allowlist (if configured)
  - It applies the first matching strategy
- Returns: a new DataFrame with transformed columns
- Logging: emits `apply.start` and `apply.summary` events (PII-safe)

### Purview-driven application (`apply_from_purview`)
Two modes:

1) Explicit parameters (back-compat):
   - Provide: `purview_name`, `tenant_id`, `client_id`, `client_secret`, `asset_type_name`, `asset_qualified_name` (and optional `api_version`, `classification_allowlist`)
   - The engine fetches column classifications and calls `apply`

2) Simplified parameters:
   - Provide: `source_folder` (one of `ServiceBus`, `Horizon`, `entraid`) plus:
     - `entity_name` (for `ServiceBus` and `entraid`), or
     - `file_name` (for `Horizon`)
   - The engine builds the Purview qualified name for an ADLS Gen2 resource-set asset, resolves the storage account host, resolves credentials, fetches classifications, and calls `apply`

Change detection and summary:
- Before applying, the engine creates `__orig__<column>` copies for selected columns
- After applying, it counts rows where any anonymised column changed
- Drops the `__orig__*` columns before returning
- Logs: `purview.resolve.start`, `purview.resolve.done`, `apply_from_purview.columns_selected`, `apply_from_purview.summary`

Credentials and defaults:
- Access token acquisition prefers client credentials; if no secret is configured, falls back to Azure Identity (`DefaultAzureCredential`).
- Client secret resolution:
  - Use `ODW_CLIENT_SECRET` if set
  - Else try Key Vault via `notebookutils.mssparkutils` using Spark conf `keyVaultName` (or `ODW_KEYVAULT_NAME`) and `ODW_PURVIEW_SECRET_NAME`
  - Else use the sentinel `AZURE_IDENTITY` to trigger `DefaultAzureCredential`
- Storage account host resolution (simplified mode):
  - Use `ODW_STORAGE_ACCOUNT_DFS_HOST` if set
  - Else try `mssparkutils.notebook.run("/utils/py_utils_get_storage_account")`
  - If unresolved, an error is raised instructing to use the explicit API
- Default environment variables used by the engine:
  - `ODW_PURVIEW_NAME`, `ODW_TENANT_ID`, `ODW_CLIENT_ID`, `ODW_STORAGE_ACCOUNT_DFS_HOST`
  - `ODW_CLIENT_SECRET`, `ODW_PURVIEW_SECRET_NAME`, `ODW_KEYVAULT_NAME`
  - `ODW_RUN_ID` (optional correlation/run ID included in logs)

## Public API
Re-exported from `odw.core.anonymisation`:
- `BaseStrategy`, `default_strategies()`
- `AnonymisationEngine`, `fetch_purview_classifications_by_qualified_name`
- `AnonymisationConfig`, `load_config`

## Usage examples

Direct application with supplied classifications:
```python
from pyspark.sql import SparkSession
from odw.core.anonymisation import AnonymisationEngine

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([
    {"EmployeeID": "E1", "full_name": "John Doe", "email": "john.doe@example.com"},
])

cols = [
    {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
    {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
]

engine = AnonymisationEngine()
out = engine.apply(df, cols)
```

Purview-driven (explicit parameters):
```python
from odw.core.anonymisation import AnonymisationEngine

engine = AnonymisationEngine()
out = engine.apply_from_purview(
    df,
    purview_name="<purview-account>",
    tenant_id="<tenant-id>",
    client_id="<app-id>",
    client_secret="<secret or AZURE_IDENTITY>",
    asset_type_name="azure_datalake_gen2_resource_set",
    asset_qualified_name="https://<dfs-host>/<container>/<path>/<file-or-pattern>",
)
```

Purview-driven (simplified parameters):
```python
# ServiceBus example (entity messages with timestamp-named files)
out = engine.apply_from_purview(df, entity_name="MyEntity", source_folder="ServiceBus")

# Horizon example (dated folder containing the file)
out = engine.apply_from_purview(df, file_name="MyExtract.csv", source_folder="Horizon")

# Entra ID example (dated folder with entity-named JSON)
out = engine.apply_from_purview(df, entity_name="groups", source_folder="entraid")
```

Using a classification allowlist from YAML:
```python
from odw.core.anonymisation import AnonymisationEngine, load_config

config = load_config(path="anonymisation.yml")
engine = AnonymisationEngine(config=config)
out = engine.apply(df, cols, classification_allowlist=None)  # engine will use config.allowlist
```

## Observability
- Structured, PII-safe logs are emitted as single-line JSON with event names
- If the shared logging utility is initialised, it is used; otherwise, standard logging is used
- Logs include counts, selected columns, strategies used, and a correlation/run ID when provided

## Extension points
- Add a new strategy by subclassing `BaseStrategy` and providing a `classification_names` set and an `apply` method
- Provide custom strategies to the engine (`AnonymisationEngine(strategies=[...])`) to override or extend behaviour
- Update the classification allowlist in config to scope which classes are acted upon

## Assumptions and limitations
- Operates on PySpark DataFrames; UDFs require Spark
- If no seed column is found, seeded transforms will be constant across the DataFrame
- NI number anonymisation is intentionally non-deterministic
- Column-to-classification matching relies on normalised names; if a column cannot be matched, it is ignored

## Tests
Unit tests cover masking behaviour, Purview integration (mocked), logging, and output ranges for seeded transforms. See:
- `odw/test/unit_test/anonymisation/test_unit_anonymisation_purview.py`
- `odw/test/unit_test/anonymisation/test_engine.py`
