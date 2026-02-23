# Anonymisation Package Integration Guide

## Overview
This guide explains how to integrate the anonymisation package (`odw.core.anonymisation`) into the new modular ETL pipeline architecture for automatic PII masking based on Azure Purview classifications.

## Current Architecture

### Modular Pipeline Structure
```
ETLProcess (base class)
├── StandardisationProcess
│   ├── ServiceBusStandardisationProcess
│   └── HorizonStandardisationProcess
├── HarmonisationProcess
│   └── ServiceBusHarmonisationProcess
└── CurationProcess
```

### Anonymisation Package
- **Location**: `odw/core/anonymisation/`
- **Components**:
  - `engine.py` - `AnonymisationEngine` with Purview integration
  - `base.py` - Strategy classes (NI Number, Email, Name, BirthDate, Age, Salary)
  - `config.py` - Configuration and YAML loader
- **Integration Points**: Works with PySpark DataFrames, driven by Purview classifications

## Integration Approaches

### Option 1: Integrate at Harmonisation Stage (Recommended)

The harmonisation stage is the **ideal place** for anonymisation because:
1. Data is cleaned and standardised
2. Schema is consistent
3. Before curated/production layer
4. Purview classifications are already available

#### Implementation Steps

**Step 1: Add Anonymisation to HarmonisationProcess Base Class**

Create: `odw/core/etl/transformation/harmonised/harmonisation_with_anonymisation.py`

```python
from odw.core.etl.transformation.harmonised.harmonisation_process import HarmonisationProcess
from odw.core.anonymisation import AnonymisationEngine, AnonymisationConfig
from odw.core.util.logging_util import LoggingUtil
from pyspark.sql import DataFrame
from typing import Optional
import os


class HarmonisationWithAnonymisation(HarmonisationProcess):
    """
    Extended harmonisation process that applies anonymisation after harmonisation transformations.
    """
    
    def __init__(self, spark, debug: bool = False, enable_anonymisation: bool = True):
        super().__init__(spark, debug)
        self.enable_anonymisation = enable_anonymisation
        self.anonymisation_engine = None
        if self.enable_anonymisation:
            self._init_anonymisation()
    
    def _init_anonymisation(self):
        """Initialize the anonymisation engine with optional config."""
        config_path = os.getenv("ODW_ANONYMISATION_CONFIG_PATH")
        config = None
        
        if config_path:
            try:
                from odw.core.anonymisation import load_config
                config = load_config(path=config_path)
                LoggingUtil().log_info(f"Loaded anonymisation config from {config_path}")
            except Exception as e:
                LoggingUtil().log_warning(f"Failed to load anonymisation config: {e}. Using defaults.")
        
        self.anonymisation_engine = AnonymisationEngine(config=config)
    
    def apply_anonymisation(
        self, 
        df: DataFrame, 
        entity_name: str, 
        source_folder: str = "ServiceBus"
    ) -> DataFrame:
        """
        Apply anonymisation to a DataFrame using Purview classifications.
        
        Args:
            df: Input DataFrame
            entity_name: Entity name for Purview lookup
            source_folder: Source folder type ('ServiceBus', 'Horizon', 'entraid')
        
        Returns:
            Anonymised DataFrame
        """
        if not self.enable_anonymisation or self.anonymisation_engine is None:
            LoggingUtil().log_info("Anonymisation is disabled, skipping")
            return df
        
        try:
            LoggingUtil().log_info(
                f"Applying anonymisation for entity '{entity_name}' from source '{source_folder}'"
            )
            
            # Use the simplified Purview API
            anonymised_df = self.anonymisation_engine.apply_from_purview(
                df=df,
                entity_name=entity_name,
                source_folder=source_folder
            )
            
            LoggingUtil().log_info("Anonymisation completed successfully")
            return anonymised_df
            
        except Exception as e:
            # Log but don't fail the pipeline if anonymisation fails
            LoggingUtil().log_error(
                f"Anonymisation failed for '{entity_name}': {str(e)}. Continuing without anonymisation."
            )
            return df
    
    def harmonise_with_anonymisation(
        self, 
        data: DataFrame, 
        entity_name: str,
        source_folder: str = "ServiceBus",
        **kwargs
    ) -> DataFrame:
        """
        Extend the harmonise method to include anonymisation.
        
        This should be called by concrete implementations after their harmonisation logic.
        """
        # Call parent harmonisation first (must be implemented by subclass)
        harmonised_data = self.harmonise(data, **kwargs)
        
        # Then apply anonymisation
        anonymised_data = self.apply_anonymisation(
            harmonised_data,
            entity_name=entity_name,
            source_folder=source_folder
        )
        
        return anonymised_data
```

**Step 2: Update ServiceBusHarmonisationProcess**

Modify: `odw/core/etl/transformation/harmonised/service_bus_harmonisation_process.py`

```python
from odw.core.etl.transformation.harmonised.harmonisation_with_anonymisation import HarmonisationWithAnonymisation
# ... other imports ...


class ServiceBusHarmonisationProcess(HarmonisationWithAnonymisation):
    """
    Service Bus harmonisation with integrated anonymisation.
    """
    
    def __init__(self, spark, debug: bool = False):
        # Check if anonymisation should be enabled (default: True)
        enable_anon = os.getenv("ODW_ENABLE_ANONYMISATION", "true").lower() == "true"
        super().__init__(spark, debug, enable_anonymisation=enable_anon)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"
    
    @classmethod
    def get_name(cls):
        return "Service Bus Harmonisation"
    
    # ... existing load_data and harmonise methods ...
    
    def process(self, **kwargs):
        start_exec_time = datetime.now()
        
        # Load parameters
        entity_name: str = self.load_parameter("entity_name", kwargs)
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        # ... load other data ...
        
        # Perform harmonisation
        new_data = self.harmonise(new_data, source_system_data, hrm_incremental_key, entity_primary_key)
        
        # Apply anonymisation (NEW)
        new_data = self.apply_anonymisation(
            new_data,
            entity_name=entity_name,
            source_folder="ServiceBus"
        )
        
        # ... rest of existing process logic ...
        
        return data_to_write, ETLSuccessResult(...)
```

**Step 3: Create Horizon Harmonisation with Anonymisation**

Create: `odw/core/etl/transformation/harmonised/horizon_harmonisation_process.py`

```python
from odw.core.etl.transformation.harmonised.harmonisation_with_anonymisation import HarmonisationWithAnonymisation
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
from datetime import datetime
from typing import Dict
import os


class HorizonHarmonisationProcess(HarmonisationWithAnonymisation):
    """
    Horizon data harmonisation with integrated anonymisation.
    """
    
    def __init__(self, spark, debug: bool = False):
        enable_anon = os.getenv("ODW_ENABLE_ANONYMISATION", "true").lower() == "true"
        super().__init__(spark, debug, enable_anonymisation=enable_anon)
        self.std_db: str = "odw_standardised_db"
        self.hrm_db: str = "odw_harmonised_db"
    
    @classmethod
    def get_name(cls):
        return "Horizon Harmonisation"
    
    def load_data(self, **kwargs):
        # Similar to ServiceBus but for Horizon
        entity_name = self.load_parameter("entity_name", kwargs)
        # Load Horizon-specific data
        return {
            "orchestration_data": orchestration_data,
            "new_data": new_records,
            "existing_data": existing_harmonised_data,
        }
    
    def harmonise(self, data: DataFrame, **kwargs) -> DataFrame:
        # Horizon-specific harmonisation logic
        # Transform Horizon data structure to harmonised schema
        return harmonised_data
    
    def process(self, **kwargs):
        start_exec_time = datetime.now()
        entity_name: str = self.load_parameter("entity_name", kwargs)
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        
        new_data: DataFrame = source_data["new_data"]
        
        # Perform harmonisation
        new_data = self.harmonise(new_data, **kwargs)
        
        # Apply anonymisation (file_name needed for Horizon)
        file_name = kwargs.get("file_name", f"{entity_name}.csv")  # Get from orchestration config
        new_data = self.apply_anonymisation(
            new_data,
            entity_name=entity_name,
            source_folder="Horizon"
            # Note: For Horizon, you may need to pass file_name instead of entity_name
            # depending on your Purview asset naming convention
        )
        
        # Prepare data to write
        data_to_write = {
            # ... harmonised table write configuration ...
        }
        
        end_exec_time = datetime.now()
        return data_to_write, ETLSuccessResult(...)
```

**Step 4: Register New Processes in Factory**

Modify: `odw/core/etl/etl_process_factory.py`

```python
from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess
from odw.core.etl.transformation.harmonised.horizon_harmonisation_process import HorizonHarmonisationProcess  # NEW
from typing import Dict, List, Set, Type


class ETLProcessFactory():
    ETL_PROCESSES: Set[Type[ETLProcess]] = {
        StandardisationProcess,
        ServiceBusStandardisationProcess,
        HorizonStandardisationProcess,
        ServiceBusHarmonisationProcess,
        HorizonHarmonisationProcess,  # NEW
    }
    
    # ... rest of factory implementation ...
```

### Option 2: Create Dedicated Anonymisation ETL Process

For more flexibility, create a standalone anonymisation process that can be run independently.

Create: `odw/core/etl/transformation/anonymisation/anonymisation_process.py`

```python
from odw.core.etl.etl_process import ETLProcess
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.anonymisation import AnonymisationEngine, load_config
from odw.core.util.logging_util import LoggingUtil
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple
import os


class AnonymisationProcess(ETLProcess):
    """
    Standalone anonymisation process that can be run independently or chained in pipelines.
    
    Usage:
    ```python
    params = {
        "entity_stage_name": "Anonymisation",
        "entity_name": "appeal-has",
        "source_folder": "ServiceBus",
        "database_name": "odw_harmonised_db",
        "table_name": "sb_appeal_has",
        "target_database": "odw_harmonised_db",  # optional, defaults to same as source
        "target_table": "sb_appeal_has",  # optional, defaults to same as source
    }
    AnonymisationProcess(spark).run(**params)
    ```
    """
    
    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)
        self.anonymisation_engine = self._init_engine()
    
    @classmethod
    def get_name(cls) -> str:
        return "Anonymisation"
    
    def _init_engine(self) -> AnonymisationEngine:
        config_path = os.getenv("ODW_ANONYMISATION_CONFIG_PATH")
        config = None
        
        if config_path:
            try:
                config = load_config(path=config_path)
                LoggingUtil().log_info(f"Loaded anonymisation config from {config_path}")
            except Exception as e:
                LoggingUtil().log_warning(f"Failed to load anonymisation config: {e}")
        
        return AnonymisationEngine(config=config)
    
    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """Load data from Synapse table."""
        database_name = self.load_parameter("database_name", kwargs)
        table_name = self.load_parameter("table_name", kwargs)
        
        LoggingUtil().log_info(f"Loading data from {database_name}.{table_name}")
        
        df = SynapseTableDataIO().read(
            spark=self.spark,
            database_name=database_name,
            table_name=table_name,
            file_format="delta"
        )
        
        return {"input_data": df}
    
    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()
        
        # Load parameters
        entity_name: str = self.load_parameter("entity_name", kwargs)
        source_folder: str = self.load_parameter("source_folder", kwargs)
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        database_name: str = self.load_parameter("database_name", kwargs)
        table_name: str = self.load_parameter("table_name", kwargs)
        
        # Optional: allow different target
        target_database = kwargs.get("target_database", database_name)
        target_table = kwargs.get("target_table", table_name)
        
        input_df: DataFrame = source_data["input_data"]
        row_count_before = input_df.count()
        
        LoggingUtil().log_info(
            f"Applying anonymisation to {row_count_before} rows from {entity_name}"
        )
        
        # Apply anonymisation
        anonymised_df = self.anonymisation_engine.apply_from_purview(
            df=input_df,
            entity_name=entity_name,
            source_folder=source_folder
        )
        
        row_count_after = anonymised_df.count()
        
        # Prepare output
        data_to_write = {
            f"{target_database}.{target_table}": {
                "data": anonymised_df,
                "storage_kind": "SynapseTable",
                "database_name": target_database,
                "table_name": target_table,
                "file_format": "delta",
                "mode": "overwrite",  # or "append" depending on use case
            }
        }
        
        end_exec_time = datetime.now()
        
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=f"{target_database}.{target_table}",
                insert_count=row_count_after,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
```

## Configuration

### Environment Variables

Set these in your Synapse notebook or pipeline:

```python
# Required for Purview integration
os.environ["ODW_PURVIEW_NAME"] = "pins-pview"
os.environ["ODW_TENANT_ID"] = "5878df98-6f88-48ab-9322-998ce557088d"
os.environ["ODW_CLIENT_ID"] = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = "<your-storage-account>.dfs.core.windows.net"

# Optional: Disable anonymisation
os.environ["ODW_ENABLE_ANONYMISATION"] = "true"  # or "false"

# Optional: Classification allowlist config
os.environ["ODW_ANONYMISATION_CONFIG_PATH"] = "/path/to/anonymisation.yml"

# Optional: Run ID for correlation
os.environ["ODW_RUN_ID"] = "run-123"
```

### Anonymisation Config (Optional)

Create `anonymisation.yml` in your config container:

```yaml
classification_allowlist:
  - MICROSOFT.PERSONAL.NAME
  - MICROSOFT.PERSONAL.EMAIL
  - NI Number
  - Birth Date
  - Person's Age
  - Annual Salary
```

## Usage Examples

### Example 1: Running appeal-has with Anonymisation in Debug Mode

```python
# In py_etl_orchestrator notebook
input_arguments = {
    "entity_stage_name": "Service Bus Harmonisation",
    "entity_name": "appeal-has",
    "debug": True
}

# The anonymisation will be applied automatically
# Check logs for: [AnonymisationEngine] events
```

### Example 2: Running Standalone Anonymisation Process

```python
from odw.core.etl.etl_process_factory import ETLProcessFactory

params = {
    "entity_stage_name": "Anonymisation",
    "entity_name": "appeal-has",
    "source_folder": "ServiceBus",
    "database_name": "odw_harmonised_db",
    "table_name": "sb_appeal_has",
}

etl_process = ETLProcessFactory.get("Anonymisation")
result = etl_process(spark).run(**params)
```

### Example 3: Disabling Anonymisation for Specific Run

```python
import os

# Temporarily disable
os.environ["ODW_ENABLE_ANONYMISATION"] = "false"

# Run harmonisation - no anonymisation will be applied
input_arguments = {
    "entity_stage_name": "Service Bus Harmonisation",
    "entity_name": "appeal-has",
}
```

## Testing

### Unit Tests

Create: `odw/test/unit_test/etl/test_anonymisation_integration.py`

```python
import pytest
from pyspark.sql import SparkSession
from odw.core.etl.transformation.harmonised.service_bus_harmonisation_process import ServiceBusHarmonisationProcess


class TestAnonymisationIntegration:
    @pytest.fixture
    def spark(self):
        return SparkSession.builder.master("local[*]").getOrCreate()
    
    def test_harmonisation_with_anonymisation_enabled(self, spark):
        process = ServiceBusHarmonisationProcess(spark, debug=True)
        assert process.enable_anonymisation is True
        assert process.anonymisation_engine is not None
    
    def test_harmonisation_with_anonymisation_disabled(self, spark, monkeypatch):
        monkeypatch.setenv("ODW_ENABLE_ANONYMISATION", "false")
        process = ServiceBusHarmonisationProcess(spark, debug=True)
        assert process.enable_anonymisation is False
```

### Integration Test

Run the validation test for appeal-has to ensure anonymisation doesn't break existing functionality:

```bash
cd /Users/stefania.deligia/Documents/PINS/odw-synapse-workspace
python -m pytest tests/validation_test/test_appeals_has.py -v
```

## Monitoring and Observability

The anonymisation engine emits structured logs. Look for:

```json
{
  "event": "apply_from_purview.start",
  "entity_name": "appeal-has",
  "source_folder": "ServiceBus"
}

{
  "event": "apply_from_purview.columns_selected",
  "columns": ["full_name", "email", "ni_number"],
  "strategies": ["NameMaskStrategy", "EmailMaskStrategy", "NINumberStrategy"]
}

{
  "event": "apply_from_purview.summary",
  "rows_changed": 150,
  "total_rows": 200
}
```

## Rollout Plan

### Phase 1: Development (Current)
1. Implement `HarmonisationWithAnonymisation` base class
2. Update `ServiceBusHarmonisationProcess`
3. Test with appeal-has in debug mode
4. Verify Purview integration

### Phase 2: Testing
1. Run integration tests
2. Validate anonymised data meets requirements
3. Performance testing (anonymisation overhead)
4. Security review

### Phase 3: Deployment
1. Deploy to dev environment
2. Enable for specific entities (allowlist)
3. Monitor logs and performance
4. Gradually enable for all entities

### Phase 4: Production
1. Deploy to test environment
2. Run validation tests
3. Deploy to production
4. Enable anonymisation by default

## Troubleshooting

### Issue: Purview Classifications Not Found
**Solution**: Verify Purview asset naming and ensure data has been classified.

```python
# Debug Purview connection
from odw.core.anonymisation import fetch_purview_classifications_by_qualified_name

qualified_name = "https://<host>/odw-raw/ServiceBus/appeal-has/{Year}-{Month}-{Day}/..."
cols = fetch_purview_classifications_by_qualified_name(
    purview_name="pins-pview",
    # ... credentials ...
    asset_qualified_name=qualified_name
)
print(cols)
```

### Issue: Anonymisation Slowing Down Pipeline
**Solution**: Use classification allowlist to limit transformations.

```yaml
# Only anonymise critical PII
classification_allowlist:
  - MICROSOFT.PERSONAL.NAME
  - NI Number
```

### Issue: Custom Data Types Not Handled
**Solution**: Create custom strategy and add to engine.

```python
from odw.core.anonymisation import BaseStrategy, AnonymisationEngine

class CustomStrategy(BaseStrategy):
    classification_names = {"Custom Classification"}
    
    def apply(self, df, column, seed, context):
        # Your logic here
        return df

engine = AnonymisationEngine(strategies=[CustomStrategy(), *default_strategies()])
```

## Summary

**Recommended Approach**: Option 1 (Harmonisation Integration)
- ✅ Automatic anonymisation at the right pipeline stage
- ✅ Uses existing Purview classifications
- ✅ Minimal code changes
- ✅ Easy to enable/disable
- ✅ Maintains pipeline modularity

**Next Steps**:
1. Create `harmonisation_with_anonymisation.py`
2. Update `service_bus_harmonisation_process.py`
3. Test with appeal-has in debug mode (see APPEAL_HAS_DEBUG_INSTRUCTIONS.md)
4. Verify anonymised output
5. Roll out to other entities

## References
- Anonymisation Package: `odw/core/anonymisation/README.md`
- ETL Process Factory: `odw/core/etl/etl_process_factory.py`
- Harmonisation Process: `odw/core/etl/transformation/harmonised/service_bus_harmonisation_process.py`
