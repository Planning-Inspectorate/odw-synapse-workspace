# Anonymisation Implementation Summary

## Branch
`THEODW-2355-Embed-anonymisation-in-py_sb_raw_to_std-notebooks-and-py_horizon_raw_to_std-for-DEV-TEST-only`

## Changes Completed

### 1. ServiceBusStandardisationProcess
**File**: `odw/core/etl/transformation/standardised/service_bus_standardisation_process.py`

**Changes**:
- Added imports for `AnonymisationEngine`, `load_config`, and `os`
- Added `__init__` method to initialize anonymisation engine
- Added `_init_anonymisation()` method to check environment variable `ODW_ENABLE_ANONYMISATION`
- Added `apply_anonymisation()` method to apply anonymisation from Purview
- Modified `process()` method to call `apply_anonymisation()` after data deduplication (line ~236)

### 2. HorizonStandardisationProcess
**File**: `odw/core/etl/transformation/standardised/horizon_standardisation_process.py`

**Changes**:
- Added imports for `AnonymisationEngine`, `load_config`, and `os`
- Added `__init__` method to initialize anonymisation engine
- Added `_init_anonymisation()` method to check environment variable `ODW_ENABLE_ANONYMISATION`
- Added `apply_anonymisation()` method to apply anonymisation from Purview
- Modified `process()` method to call `apply_anonymisation()` after type casting (line ~267)

## Configuration Requirements

### Environment Variables
Set these in Synapse or your local environment before running:

```python
# Required for anonymisation
os.environ["ODW_ENABLE_ANONYMISATION"] = "true"

# Required for Purview integration
os.environ["ODW_PURVIEW_NAME"] = "pins-pview"
os.environ["ODW_TENANT_ID"] = "5878df98-6f88-48ab-9322-998ce557088d"
os.environ["ODW_CLIENT_ID"] = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = "pinsstodwdevuks9h80mb.dfs.core.windows.net"

# Optional
os.environ["ODW_RUN_ID"] = "appeal-has-anon-test"
os.environ["ODW_ANONYMISATION_CONFIG_PATH"] = "/path/to/anonymisation.yml"  # if using custom config
```

## Testing Instructions

### 1. Set Environment Variables
In a Synapse notebook cell, run:

```python
import os
os.environ["ODW_ENABLE_ANONYMISATION"] = "true"
os.environ["ODW_PURVIEW_NAME"] = "pins-pview"
os.environ["ODW_TENANT_ID"] = "5878df98-6f88-48ab-9322-998ce557088d"
os.environ["ODW_CLIENT_ID"] = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = "pinsstodwdevuks9h80mb.dfs.core.windows.net"
os.environ["ODW_RUN_ID"] = "appeal-has-anon-test"
```

### 2. Test Service Bus Standardisation
Run `py_etl_orchestrator` with:

```python
input_arguments = {
    "entity_stage_name": "Service Bus Standardisation",
    "entity_name": "appeal-has",
    "debug": True
}
```

### 3. Test Horizon Standardisation
Run `py_etl_orchestrator` with:

```python
input_arguments = {
    "entity_stage_name": "Horizon Standardisation",
    "entity_name": "appeal-has",
    "debug": True
}
```

### 4. Verify Logs
Look for log messages like:
- `[ServiceBusStandardisationProcess] Anonymisation engine initialized`
- `[HorizonStandardisationProcess] Anonymisation engine initialized`
- `[AnonymisationEngine] {"event": "apply_from_purview.start", ...}`
- `[AnonymisationEngine] {"event": "apply_from_purview.columns_selected", ...}`
- `[AnonymisationEngine] {"event": "apply_from_purview.summary", "rows_changed": X, ...}`

### 5. Verify Data
Check the standardised tables to ensure sensitive data is masked:
- Email addresses should be masked (e.g., `j****@example.com`)
- Names should be masked (e.g., `J*** D**`)
- NI numbers should be masked
- Birth dates should be replaced with age ranges

## Purview Prerequisites

Before testing, ensure:
1. ✅ Asset exists in Purview for `odw-raw/ServiceBus/appeal-has/` and `odw-raw/Horizon/appeal-has/`
2. ✅ Classifications applied to sensitive columns
3. ✅ Service principal has Data Reader role in Purview
4. ✅ Storage account scanned in Purview

See `PURVIEW_READINESS_CHECKLIST.md` for full details.

## Fallback (if Purview not ready)

If Purview integration is not working, you can use explicit classifications:

```python
from odw.core.anonymisation import AnonymisationEngine

engine = AnonymisationEngine()
columns_with_classifications = [
    {"column_name": "appellant_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
    {"column_name": "email_address", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
    {"column_name": "ni_number", "classifications": ["NI Number"]},
]
anonymised_df = engine.apply(df, columns_with_classifications)
```

## Next Steps

1. ✅ Service Bus standardisation updated
2. ✅ Horizon standardisation updated
3. ⏳ Test with appeal-has entity
4. ⏳ Verify Purview prerequisites
5. ⏳ Commit changes with appropriate message
6. ⏳ Document any issues or adjustments needed

## Notes

- Anonymisation only runs when `ODW_ENABLE_ANONYMISATION=true`
- If anonymisation fails, the original DataFrame is returned (fail-safe)
- Logs include detailed information about which columns were anonymised
- The implementation is backward compatible - existing pipelines work without changes
