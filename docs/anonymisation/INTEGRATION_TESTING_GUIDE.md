# Anonymisation Integration Tests

This directory contains integration tests for the anonymisation engine that make real Purview API calls with Azure authentication.

## Overview

The integration tests validate:
- Real Purview API calls with Azure authentication
- End-to-end anonymisation with actual classified columns from Purview
- Classification-based data transformation
- Simplified and explicit API modes
- Classification allowlist filtering
- Deterministic behavior with seed columns
- Null value preservation
- Change tracking and logging

## Prerequisites

### 1. Azure Credentials

You need valid Azure credentials to authenticate with Purview. You can use:

- **Service Principal (Recommended for CI/CD)**
  - Set `ODW_TENANT_ID`, `ODW_CLIENT_ID`, `ODW_CLIENT_SECRET`
  
- **DefaultAzureCredential (Local Development)**
  - Set `ODW_TENANT_ID`, `ODW_CLIENT_ID`
  - Set `ODW_CLIENT_SECRET=AZURE_IDENTITY` or leave it unset
  - Ensure you're logged in via Azure CLI: `az login`

### 2. Purview Access

- Your credentials must have read access to the Purview account
- The test assumes access to the `pins-pview` Purview account (configurable)
- Test assets must be scanned and classified in Purview

### 3. Test Data

The tests use the `service-user` entity in the ServiceBus folder as the default test asset. This asset should:
- Exist in your Purview catalog
- Be scanned and classified
- Have columns like `emailAddress`, `full_name`, `Age`, etc. with appropriate classifications

Default asset FQN:
```
https://pinsstodwdevuks9h80mb.dfs.core.windows.net/odw-raw/ServiceBus/service-user/{Year}-{Month}-{Day}/service-user_{Year}-{Month}-{Day}T{Hour}:{N}:{N}.{N}+{N}:{N}.json
```

## Environment Variables

Set these before running tests:

### Required
```bash
export ODW_TENANT_ID="your-tenant-id"
export ODW_CLIENT_ID="your-client-id"
```

### Optional
```bash
# Service principal secret (or use AZURE_IDENTITY for DefaultAzureCredential)
export ODW_CLIENT_SECRET="your-client-secret"

# Purview account name (default: pins-pview)
export ODW_PURVIEW_NAME="pins-pview"

# Storage account DFS hostname
export ODW_STORAGE_ACCOUNT_DFS_HOST="pinsstodwdevuks9h80mb.dfs.core.windows.net"

# Custom test asset (overrides default)
export PURVIEW_TEST_ASSET_QUALIFIED_NAME="https://storage.dfs.core.windows.net/container/path/file.csv"
export PURVIEW_TEST_ASSET_TYPE="azure_datalake_gen2_resource_set"
```

## Running the Tests

### Run all integration tests
```bash
pytest odw/test/integration_test/anonymisation/ -v
```

### Run only Purview-marked tests
```bash
pytest odw/test/integration_test/anonymisation/ -m purview -v
```

### Run a specific test
```bash
pytest odw/test/integration_test/anonymisation/test_integration_purview_anonymisation.py::TestPurviewAnonymisationIntegration::test_fetch_purview_classifications_real_api -v
```

### Run with detailed output
```bash
pytest odw/test/integration_test/anonymisation/ -v -s
```

### Skip integration tests (for unit test runs)
```bash
pytest -m "not integration" odw/test/unit_test/
```

## Test Descriptions

### `test_fetch_purview_classifications_real_api`
Tests basic Purview API connectivity and classification retrieval.
- Authenticates with Purview
- Resolves test asset by qualified name
- Fetches and validates column classifications

### `test_apply_from_purview_explicit_params`
Tests end-to-end anonymisation with explicit Purview parameters.
- Creates test DataFrame with sensitive columns
- Applies anonymisation using real Purview classifications
- Validates data transformations (email masking, name masking, age anonymisation, NI number generation)

### `test_apply_from_purview_simplified_horizon`
Tests the simplified API for Horizon source folders.
- Uses automatic qualified name construction
- Tests credential resolution from environment
- Validates simplified parameter interface

### `test_apply_from_purview_with_allowlist`
Tests classification allowlist filtering.
- Only processes specified classifications
- Validates that other classifications are ignored

### `test_anonymisation_determinism`
Tests deterministic behavior with seed columns.
- Applies anonymisation twice to same input
- Validates identical outputs

### `test_anonymisation_preserves_nulls`
Tests null value handling.
- Validates null values remain null
- Validates non-null values are transformed

### `test_azure_identity_credential_fallback`
Tests DefaultAzureCredential authentication.
- Uses `AZURE_IDENTITY` sentinel
- Validates managed identity or Azure CLI authentication

### `test_change_tracking_and_logging`
Tests change tracking functionality.
- Validates helper columns are removed
- Counts rows that were modified
- Tests logging with run ID

## CI/CD Integration

### GitHub Actions Example
```yaml
name: Integration Tests

on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
      
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest
      
      - name: Run integration tests
        env:
          ODW_TENANT_ID: ${{ secrets.AZURE_TENANT_ID }}
          ODW_CLIENT_ID: ${{ secrets.AZURE_CLIENT_ID }}
          ODW_CLIENT_SECRET: ${{ secrets.AZURE_CLIENT_SECRET }}
          ODW_PURVIEW_NAME: ${{ secrets.PURVIEW_NAME }}
          ODW_STORAGE_ACCOUNT_DFS_HOST: ${{ secrets.STORAGE_HOST }}
        run: |
          pytest odw/test/integration_test/anonymisation/ -m purview -v
```

### Azure DevOps Example
```yaml
steps:
- task: UsePythonVersion@0
  inputs:
    versionSpec: '3.10'

- script: |
    pip install -r requirements.txt
    pip install pytest
  displayName: 'Install dependencies'

- script: |
    pytest odw/test/integration_test/anonymisation/ -m purview -v
  displayName: 'Run Purview integration tests'
  env:
    ODW_TENANT_ID: $(AZURE_TENANT_ID)
    ODW_CLIENT_ID: $(AZURE_CLIENT_ID)
    ODW_CLIENT_SECRET: $(AZURE_CLIENT_SECRET)
    ODW_PURVIEW_NAME: $(PURVIEW_NAME)
    ODW_STORAGE_ACCOUNT_DFS_HOST: $(STORAGE_HOST)
```

## Troubleshooting

### Authentication Errors
```
Exception: Token request failed [401]: Unauthorized
```
**Solution**: Verify your service principal credentials and ensure they have Purview Data Reader role.

### Asset Not Found
```
Exception: Could not resolve GUID for azure_datalake_gen2_resource_set :: <qualified_name>
```
**Solution**: Verify the asset exists in Purview and the qualified name is correct. Ensure the asset has been scanned.

### No Classifications Found
```
Fetched 0 classified columns from Purview
```
**Solution**: The asset may not have been scanned yet, or classifications haven't been applied. Run a Purview scan on the asset.

### DefaultAzureCredential Fails
```
DefaultAzureCredential not available in this environment
```
**Solution**: Either:
- Run `az login` to authenticate locally
- Configure managed identity if running on Azure
- Use service principal with `ODW_CLIENT_SECRET` instead

## Local Development

For local testing without modifying environment:

```python
# Set environment inline for a single test run
import os
os.environ.update({
    'ODW_TENANT_ID': 'your-tenant-id',
    'ODW_CLIENT_ID': 'your-client-id',
    'ODW_CLIENT_SECRET': 'AZURE_IDENTITY',  # Use Azure CLI auth
})

# Run tests
pytest odw/test/integration_test/anonymisation/ -v
```

## Notes

- Integration tests are marked with `@pytest.mark.integration` and `@pytest.mark.purview`
- Tests will skip automatically if required credentials are not available
- Tests use the existing `DaRT_LPA.csv` asset by default but can be configured for other assets
- Real API calls may take a few seconds per test
- Consider rate limiting when running tests frequently
- Test data is synthetic and no real PII is used or exposed

## Related Documentation

- [Anonymisation Engine README](../../../core/anonymisation/README.md)
- [Unit Tests](../../unit_test/anonymisation/)
- [Test Script](../../../../scripts/test_purview_mask.py)
