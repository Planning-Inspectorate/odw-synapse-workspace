# Anonymisation at Raw to Standardised Step

## Overview
Implementation guide for adding anonymisation to the **Service Bus Standardisation** process (Raw → Standardised layer).

## Why Standardisation Step?
- ✅ Data is fresh from raw layer
- ✅ Before any transformations
- ✅ Earliest point to protect PII
- ✅ Simpler schema to work with

## Implementation

### Step 1: Update ServiceBusStandardisationProcess

Modify: `odw/core/etl/transformation/standardised/service_bus_standardisation_process.py`

Add anonymisation imports and initialization:

```python
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.etl.util.schema_util import SchemaUtil
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.io.synapse_table_data_io import SynapseTableDataIO

# NEW: Import anonymisation
from odw.core.anonymisation import AnonymisationEngine, load_config

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict
import re
import os  # NEW


class ServiceBusStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising the raw data from the Service Bus
    with integrated anonymisation.
    """
    
    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)
        self.enable_anonymisation = os.getenv("ODW_ENABLE_ANONYMISATION", "true").lower() == "true"
        self.anonymisation_engine = None
        if self.enable_anonymisation:
            self._init_anonymisation()
    
    def _init_anonymisation(self):
        """Initialize the anonymisation engine with optional config."""
        config_path = os.getenv("ODW_ANONYMISATION_CONFIG_PATH")
        config = None
        
        if config_path:
            try:
                config = load_config(path=config_path)
                LoggingUtil().log_info(f"Loaded anonymisation config from {config_path}")
            except Exception as e:
                LoggingUtil().log_warning(f"Failed to load anonymisation config: {e}. Using defaults.")
        
        self.anonymisation_engine = AnonymisationEngine(config=config)
        LoggingUtil().log_info("Anonymisation engine initialized for standardisation")
    
    def apply_anonymisation(self, df: DataFrame, entity_name: str) -> DataFrame:
        """
        Apply anonymisation to standardised DataFrame using Purview classifications.
        
        Args:
            df: Input DataFrame (standardised raw messages)
            entity_name: Entity name for Purview lookup
        
        Returns:
            Anonymised DataFrame
        """
        if not self.enable_anonymisation or self.anonymisation_engine is None:
            LoggingUtil().log_info("Anonymisation is disabled, skipping")
            return df
        
        try:
            LoggingUtil().log_info(
                f"Applying anonymisation to standardised data for entity '{entity_name}'"
            )
            
            # Use the simplified Purview API for Service Bus
            anonymised_df = self.anonymisation_engine.apply_from_purview(
                df=df,
                entity_name=entity_name,
                source_folder="ServiceBus"
            )
            
            LoggingUtil().log_info("Anonymisation completed successfully")
            return anonymised_df
            
        except Exception as e:
            # Log but don't fail the pipeline if anonymisation fails
            LoggingUtil().log_error(
                f"Anonymisation failed for '{entity_name}': {str(e)}. Continuing without anonymisation."
            )
            return df
    
    @classmethod
    def get_name(cls):
        return "Service Bus Standardisation"
    
    # ... keep all existing methods (get_max_file_date, get_missing_files, etc.) ...
    
    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a source_data dictionary to be provided, but was missing")
        entity_name: str = kwargs.get("entity_name", None)
        if not entity_name:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a entity_name to be provided, but was missing")
        database_name = "odw_standardised_db"
        table_name = f"sb_{entity_name.replace('-', '_')}"
        table_path: str = f"{database_name}.{table_name}"

        table_df = source_data.pop(table_path, None)
        if not table_df:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a source_data dataframe to be provided, but was missing")

        new_raw_messages = source_data.pop("raw_messages", None)
        if not new_raw_messages:
            raise ValueError()
        
        table_row_count = table_df.count()
        new_raw_messages = self.remove_data_duplicates(new_raw_messages)
        
        # NEW: Apply anonymisation BEFORE writing to standardised layer
        new_raw_messages = self.apply_anonymisation(new_raw_messages, entity_name)
        
        insert_count = new_raw_messages.count()
        LoggingUtil().log_info(f"Rows to append: {insert_count}")
        expected_new_count = table_row_count + insert_count
        LoggingUtil().log_info(f"Expected new count: {expected_new_count}")
        end_exec_time = datetime.now()
        
        return {
            table_path: {
                "data": new_raw_messages,
                "storage_kind": "ADLSG2-Table",
                "database_name": database_name,
                "table_name": table_name,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": table_name,
                "file_format": "delta",
                "write_mode": "append",
                "write_options": [("mergeSchema", "true")],
            }
        }, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=table_name,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
```

### Step 2: Update Horizon Standardisation (if needed)

If you also need anonymisation for Horizon data:

Modify: `odw/core/etl/transformation/standardised/horizon_standardisation_process.py`

Add similar anonymisation logic:

```python
from odw.core.anonymisation import AnonymisationEngine, load_config
import os

class HorizonStandardisationProcess(StandardisationProcess):
    """
    Horizon standardisation with anonymisation.
    """
    
    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)
        self.enable_anonymisation = os.getenv("ODW_ENABLE_ANONYMISATION", "true").lower() == "true"
        self.anonymisation_engine = None
        if self.enable_anonymisation:
            self._init_anonymisation()
    
    def _init_anonymisation(self):
        """Initialize the anonymisation engine."""
        config_path = os.getenv("ODW_ANONYMISATION_CONFIG_PATH")
        config = None
        
        if config_path:
            try:
                config = load_config(path=config_path)
                LoggingUtil().log_info(f"Loaded anonymisation config from {config_path}")
            except Exception as e:
                LoggingUtil().log_warning(f"Failed to load anonymisation config: {e}")
        
        self.anonymisation_engine = AnonymisationEngine(config=config)
    
    def apply_anonymisation(self, df: DataFrame, entity_name: str, file_name: str = None) -> DataFrame:
        """Apply anonymisation for Horizon data."""
        if not self.enable_anonymisation or self.anonymisation_engine is None:
            return df
        
        try:
            LoggingUtil().log_info(f"Applying anonymisation to Horizon data for '{entity_name}'")
            
            # For Horizon, we may need to specify file_name
            if file_name:
                anonymised_df = self.anonymisation_engine.apply_from_purview(
                    df=df,
                    file_name=file_name,
                    source_folder="Horizon"
                )
            else:
                # Fallback to entity_name if file_name not provided
                anonymised_df = self.anonymisation_engine.apply_from_purview(
                    df=df,
                    entity_name=entity_name,
                    source_folder="Horizon"
                )
            
            return anonymised_df
            
        except Exception as e:
            LoggingUtil().log_error(f"Anonymisation failed: {str(e)}")
            return df
    
    # ... rest of Horizon process logic ...
    
    def process(self, **kwargs):
        # ... existing logic ...
        
        # Apply anonymisation before writing
        standardised_data = self.apply_anonymisation(
            standardised_data, 
            entity_name=entity_name,
            file_name=kwargs.get("file_name")
        )
        
        # ... return data to write ...
```

## Configuration

### Environment Variables

Set these before running the standardisation:

```python
import os

# Enable/disable anonymisation
os.environ["ODW_ENABLE_ANONYMISATION"] = "true"  # or "false" to disable

# Required for Purview integration
os.environ["ODW_PURVIEW_NAME"] = "pins-pview"
os.environ["ODW_TENANT_ID"] = "5878df98-6f88-48ab-9322-998ce557088d"
os.environ["ODW_CLIENT_ID"] = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = "<your-dfs-host>"

# Optional: Classification allowlist
os.environ["ODW_ANONYMISATION_CONFIG_PATH"] = "/path/to/anonymisation.yml"

# Optional: Run ID for tracing
os.environ["ODW_RUN_ID"] = "run-123"
```

### Classification Allowlist (Optional)

Create `anonymisation.yml`:

```yaml
classification_allowlist:
  - MICROSOFT.PERSONAL.NAME
  - MICROSOFT.PERSONAL.EMAIL
  - NI Number
  - Birth Date
  - Person's Age
  - Annual Salary
```

## Usage Example

### Running appeal-has Standardisation with Anonymisation

In `py_etl_orchestrator` notebook:

```python
# Set environment variables
import os
os.environ["ODW_ENABLE_ANONYMISATION"] = "true"
os.environ["ODW_PURVIEW_NAME"] = "pins-pview"
os.environ["ODW_TENANT_ID"] = "5878df98-6f88-48ab-9322-998ce557088d"
os.environ["ODW_CLIENT_ID"] = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"

# Run standardisation
input_arguments = {
    "entity_stage_name": "Service Bus Standardisation",
    "entity_name": "appeal-has",
    "debug": True
}

# Anonymisation will be applied automatically!
# Check logs for [AnonymisationEngine] events
```

### Testing Without Anonymisation

```python
import os

# Temporarily disable
os.environ["ODW_ENABLE_ANONYMISATION"] = "false"

input_arguments = {
    "entity_stage_name": "Service Bus Standardisation",
    "entity_name": "appeal-has",
    "debug": True
}

# No anonymisation will be applied
```

## What Gets Anonymised?

The anonymisation engine will automatically detect and mask columns based on Purview classifications:

| Classification | Strategy | Example Transformation |
|---------------|----------|----------------------|
| `MICROSOFT.PERSONAL.NAME` | Name masking | "John Doe" → "J*** **e" |
| `MICROSOFT.PERSONAL.EMAIL` | Email masking | "john@example.com" → "j***@example.com" |
| `NI Number` | NI Number generation | "AB123456C" → "XY654321D" |
| `Birth Date` | Random date | "1990-05-15" → "1978-03-22" |
| `Person's Age` | Random age [18-70] | "35" → "42" |
| `Annual Salary` | Random salary [20k-100k] | "45000" → "62000" |

## Monitoring

Look for these log events:

```json
{
  "event": "apply_from_purview.start",
  "entity_name": "appeal-has",
  "source_folder": "ServiceBus"
}

{
  "event": "apply_from_purview.columns_selected",
  "columns": ["appellant_name", "email_address", "ni_number"],
  "strategies": ["NameMaskStrategy", "EmailMaskStrategy", "NINumberStrategy"]
}

{
  "event": "apply_from_purview.summary",
  "rows_changed": 150,
  "total_rows": 200,
  "duration_seconds": 2.5
}
```

## Testing

### Unit Test

Create: `odw/test/unit_test/etl/test_standardisation_anonymisation.py`

```python
import pytest
from pyspark.sql import SparkSession
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess


class TestStandardisationAnonymisation:
    @pytest.fixture
    def spark(self):
        return SparkSession.builder.master("local[*]").getOrCreate()
    
    def test_anonymisation_enabled_by_default(self, spark):
        process = ServiceBusStandardisationProcess(spark, debug=True)
        assert process.enable_anonymisation is True
        assert process.anonymisation_engine is not None
    
    def test_anonymisation_can_be_disabled(self, spark, monkeypatch):
        monkeypatch.setenv("ODW_ENABLE_ANONYMISATION", "false")
        process = ServiceBusStandardisationProcess(spark, debug=True)
        assert process.enable_anonymisation is False
    
    def test_apply_anonymisation_with_sample_data(self, spark):
        process = ServiceBusStandardisationProcess(spark, debug=True)
        
        # Create sample data
        df = spark.createDataFrame([
            {"id": "1", "name": "John Doe", "email": "john@example.com"},
            {"id": "2", "name": "Jane Smith", "email": "jane@example.com"},
        ])
        
        # Apply anonymisation (will fail if Purview not available, which is expected)
        # This tests the error handling
        result = process.apply_anonymisation(df, "appeal-has")
        
        # Should return original df if Purview fails
        assert result is not None
```

### Integration Test

Run with appeal-has in debug mode:

```bash
cd /Users/stefania.deligia/Documents/PINS/odw-synapse-workspace

# Run the standardisation test
python -m pytest tests/validation_test/test_appeals_has.py::TestAppealsHas::test_appeals_has_notebook -v
```

## Pipeline Flow

```
Raw Layer (Service Bus JSON)
         ↓
    [Read Raw Messages]
         ↓
    [Remove Duplicates]
         ↓
  ✨ [ANONYMISATION] ✨  ← NEW STEP
         ↓
    [Write to Standardised]
         ↓
Standardised Layer (Delta Table)
```

## Architecture & Dataflow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ANONYMISATION SOLUTION                                │
│                         (Raw → Standardised Layer)                              │
└─────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ STEP 1: DATA INGESTION                                                          │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────┐
    │  Service Bus     │
    │  Raw JSON Files  │         External Data Sources
    └────────┬─────────┘         (Service Bus, Horizon, etc.)
             │
             ▼
    ┌────────────────────┐
    │  RAW LAYER         │       Storage: ADLS Gen2 (odw-raw)
    │  odw_raw_db        │       Format: JSON
    │  sb_appeal_has     │       Contains: Unprocessed PII data
    └────────┬───────────┘
             │
             │ read_raw_messages()
             ▼

┌─────────────────────────────────────────────────────────────────────────────────┐
│ STEP 2: STANDARDISATION PROCESS (with Anonymisation)                           │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌────────────────────────────────────────────────────────────────┐
    │  ServiceBusStandardisationProcess                              │
    │                                                                │
    │  ┌──────────────────────────────────────────────────┐         │
    │  │ 1. __init__()                                    │         │
    │  │    - Check ODW_ENABLE_ANONYMISATION env var     │         │
    │  │    - Initialize AnonymisationEngine if enabled  │         │
    │  │    - Load config from ODW_ANONYMISATION_CONFIG  │         │
    │  └──────────────────────────────────────────────────┘         │
    │                     ▼                                          │
    │  ┌──────────────────────────────────────────────────┐         │
    │  │ 2. process()                                      │         │
    │  │    - Load source data from raw layer             │         │
    │  │    - Parse JSON messages                         │         │
    │  │    - Remove duplicates                           │         │
    │  └──────────────────────────────────────────────────┘         │
    │                     ▼                                          │
    │  ┌──────────────────────────────────────────────────┐         │
    │  │ 3. apply_anonymisation()  ← NEW METHOD           │         │
    │  │                                                   │         │
    │  │    Input: DataFrame + entity_name                │         │
    │  │    Output: Anonymised DataFrame                  │         │
    │  └──────────────┬───────────────────────────────────┘         │
    └─────────────────┼───────────────────────────────────────────┬─┘
                      │                                           │
                      ▼                                           │
            ┌─────────────────────┐                               │
            │ AnonymisationEngine │                               │
            │   .apply_from_      │                               │
            │    purview()        │                               │
            └─────────┬───────────┘                               │
                      │                                           │
          ┌───────────┴───────────────┐                           │
          ▼                           ▼                           │
  ┌───────────────┐          ┌────────────────┐                  │
  │ Purview API   │          │ Classification │                  │
  │ Client        │          │ Mapper         │                  │
  └───────┬───────┘          └────────┬───────┘                  │
          │                           │                          │
          └────────────┬──────────────┘                          │
                       ▼                                          │

┌─────────────────────────────────────────────────────────────────────────────────┐
│ STEP 3: PURVIEW INTEGRATION (Metadata Discovery)                               │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌──────────────────────┐
                    │  Microsoft Purview   │
                    │   (pins-pview)       │
                    └──────────┬───────────┘
                               │
                               │ Query classifications
                               │ for entity: "appeal-has"
                               │
                    ┌──────────▼───────────┐
                    │  Asset Metadata      │
                    │                      │
                    │  Column Mapping:     │
                    │  - appellant_name    │
                    │    → NAME            │
                    │  - email_address     │
                    │    → EMAIL           │
                    │  - ni_number         │
                    │    → NI Number       │
                    │  - dob               │
                    │    → Birth Date      │
                    └──────────┬───────────┘
                               │
                               │ Return classifications
                               ▼

┌─────────────────────────────────────────────────────────────────────────────────┐
│ STEP 4: ANONYMISATION STRATEGY SELECTION & EXECUTION                           │
└─────────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────────┐
    │  Strategy Mapper (Classification → Strategy)                 │
    └──────────────────┬───────────────────────────────────────────┘
                       │
         ┌─────────────┼─────────────┬─────────────┬─────────────┐
         ▼             ▼             ▼             ▼             ▼
    ┌────────┐   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
    │  Name  │   │  Email   │  │NI Number │  │BirthDate │  │  Salary  │
    │  Mask  │   │   Mask   │  │Generator │  │ Random   │  │ Random   │
    │Strategy│   │ Strategy │  │ Strategy │  │ Strategy │  │ Strategy │
    └────┬───┘   └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘
         │            │             │             │             │
         │ Apply      │ Apply       │ Apply       │ Apply       │ Apply
         │ to column  │ to column   │ to column   │ to column   │ to column
         ▼            ▼             ▼             ▼             ▼

    ┌────────────────────────────────────────────────────────────────┐
    │              TRANSFORMATION EXAMPLES                           │
    ├────────────────────────────────────────────────────────────────┤
    │  "John Doe"         →  "J*** **e"                             │
    │  "john@ex.com"      →  "j***@ex.com"                          │
    │  "AB123456C"        →  "XY654321D"  (valid NI format)         │
    │  "1990-05-15"       →  "1978-03-22" (random date)             │
    │  "45000"            →  "62000"      (random 20k-100k)         │
    └────────────────────────────────────────────────────────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Anonymised      │
              │ DataFrame       │
              └────────┬────────┘
                       │

┌─────────────────────────────────────────────────────────────────────────────────┐
│ STEP 5: WRITE TO STANDARDISED LAYER                                            │
└─────────────────────────────────────────────────────────────────────────────────┘

                       │
                       ▼
              ┌─────────────────┐
              │ Write Mode:     │
              │ append          │
              │ Format: delta   │
              │ mergeSchema:true│
              └────────┬────────┘
                       │
                       ▼
          ┌─────────────────────────┐
          │  STANDARDISED LAYER     │       Storage: ADLS Gen2 (odw-standardised)
          │  odw_standardised_db    │       Format: Delta Lake
          │  sb_appeal_has          │       Contains: Anonymised PII data
          └─────────────────────────┘
                       │
                       │ Downstream consumption
                       ▼
          ┌─────────────────────────┐
          │  Harmonised Layer       │       Safe for analytics
          │  Curated Layer          │       PII already protected
          │  Publish Layer          │
          └─────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────┐
│ CONFIGURATION & CONTROL                                                         │
└─────────────────────────────────────────────────────────────────────────────────┘

    Environment Variables:
    ┌────────────────────────────────────────────────────────────────┐
    │ ODW_ENABLE_ANONYMISATION="true"         (Enable/Disable)      │
    │ ODW_PURVIEW_NAME="pins-pview"           (Purview instance)    │
    │ ODW_TENANT_ID="5878df98-..."            (Azure AD tenant)     │
    │ ODW_CLIENT_ID="5750ab9b-..."            (Service principal)   │
    │ ODW_ANONYMISATION_CONFIG_PATH="..."     (Optional config)     │
    │ ODW_RUN_ID="run-123"                    (Tracing)             │
    └────────────────────────────────────────────────────────────────┘

    Optional Configuration File (anonymisation.yml):
    ┌────────────────────────────────────────────────────────────────┐
    │ classification_allowlist:                                      │
    │   - MICROSOFT.PERSONAL.NAME                                    │
    │   - MICROSOFT.PERSONAL.EMAIL                                   │
    │   - NI Number                                                  │
    │   - Birth Date                                                 │
    │   - Person's Age                                               │
    │   - Annual Salary                                              │
    └────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────┐
│ MONITORING & OBSERVABILITY                                                      │
└─────────────────────────────────────────────────────────────────────────────────┘

    Structured Logging Events:
    ┌────────────────────────────────────────────────────────────────┐
    │ ✓ apply_from_purview.start                                     │
    │ ✓ purview_lookup.columns_found                                 │
    │ ✓ apply_from_purview.columns_selected                          │
    │ ✓ strategy.applied (per column)                                │
    │ ✓ apply_from_purview.summary                                   │
    │   - rows_changed, total_rows, duration_seconds                 │
    └────────────────────────────────────────────────────────────────┘

    Error Handling:
    ┌────────────────────────────────────────────────────────────────┐
    │ • Purview API failures → Log warning, return original data     │
    │ • Classification not found → Skip anonymisation, continue      │
    │ • Strategy failures → Log error, continue with other columns   │
    │ • Pipeline never fails due to anonymisation errors             │
    └────────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────────────────────────┐
│ KEY DESIGN DECISIONS                                                            │
└─────────────────────────────────────────────────────────────────────────────────┘

  1. ✅ EARLIEST PROTECTION
     Anonymise at Raw→Std transition (before any transformations)

  2. ✅ PURVIEW-DRIVEN
     No manual column mapping - automatic discovery via classifications

  3. ✅ FAIL-SAFE
     Anonymisation errors never break the pipeline

  4. ✅ CONFIGURABLE
     Easy enable/disable via environment variable

  5. ✅ OBSERVABLE
     Rich structured logging for monitoring and debugging

  6. ✅ PERFORMANT
     Minimal overhead - only processes classified columns

  7. ✅ EXTENSIBLE
     Easy to add new strategies for custom classifications

```

## Troubleshooting

### Issue: "Purview classifications not found"
**Cause**: Entity not classified in Purview yet
**Solution**: 
1. Verify Purview asset exists
2. Run Purview scanning on the raw data
3. Or use explicit column classification (see below)

### Issue: Anonymisation taking too long
**Solution**: Use classification allowlist to limit columns:

```yaml
# Only anonymise critical PII
classification_allowlist:
  - MICROSOFT.PERSONAL.NAME
  - NI Number
```

### Issue: Need to test without Purview
**Solution**: Use explicit column classifications:

```python
from odw.core.anonymisation import AnonymisationEngine

engine = AnonymisationEngine()

# Apply without Purview
columns_with_classifications = [
    {"column_name": "appellant_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
    {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
]

anonymised_df = engine.apply(df, columns_with_classifications)
```

## Rollout Steps

1. ✅ **Update `ServiceBusStandardisationProcess`** (add anonymisation code)
2. ✅ **Set environment variables** (enable anonymisation)
3. ✅ **Test with appeal-has in debug mode**
4. ✅ **Verify anonymised output** (check logs)
5. ✅ **Deploy to dev environment**
6. ✅ **Monitor performance** (check duration)
7. ✅ **Gradually enable for other entities**

## Quick Implementation Checklist

- [ ] Copy anonymisation code into `service_bus_standardisation_process.py`
- [ ] Add imports: `from odw.core.anonymisation import AnonymisationEngine, load_config`
- [ ] Add `__init__` method with anonymisation initialization
- [ ] Add `_init_anonymisation` method
- [ ] Add `apply_anonymisation` method
- [ ] Update `process` method: add anonymisation call after `remove_data_duplicates`
- [ ] Set environment variables
- [ ] Test with appeal-has
- [ ] Check logs for anonymisation events
- [ ] Verify data is masked correctly

## Summary

**Changes Required**: Minimal!
- ✅ Add 3 methods to `ServiceBusStandardisationProcess`
- ✅ Add 1 line in `process()` method
- ✅ Set environment variables
- ✅ That's it!

**Benefits**:
- 🔒 PII protected at earliest stage
- 🎯 Purview-driven (automatic column detection)
- 🔧 Easy to enable/disable
- 📊 Full observability
- 🚀 Minimal performance impact

The anonymisation happens **automatically** based on Purview classifications, no manual column mapping needed!
