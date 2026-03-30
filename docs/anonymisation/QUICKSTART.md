# Quick Start: Purview Integration Tests

## Minimal Setup

### 1. Set Required Environment Variables

```bash
export ODW_TENANT_ID="5878df98-6f88-48ab-9322-998ce557088d"export ODW_CLIENT_ID="5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"export ODW_CLIENT_SECRET="AZURE_IDENTITY"  # Or your service principal secret
```

### 2. Authenticate with Azure (if using DefaultAzureCredential)

```bash
az login
```

### 3. Run the Tests

```bash
# Run all integration testspytest odw/test/integration_test/anonymisation/ -v -s# Or just run the basic connectivity testpytest odw/test/integration_test/anonymisation/test_integration_purview_anonymisation.py::TestPurviewAnonymisationIntegration::test_fetch_purview_classifications_real_api -v -s
```

## Expected Output

Successful test run:

```
test_fetch_purview_classifications_real_api PASSEDFetched 6 classified columns from Purview  - emailAddress: ['MICROSOFT.PERSONAL.EMAIL']  - full_name: ['MICROSOFT.PERSONAL.NAME']  - Age: ["Person's Age"]  - NINumber: ['NI Number']  - BirthDate: ['Birth Date']  - AnnualSalary: ['Annual Salary']
```

## Common Issues

### "Purview integration tests require ODW_TENANT_ID and ODW_CLIENT_ID"

→ Set the required environment variables (see step 1)

### "DefaultAzureCredential not available in this environment"

→ Run `az login` or set `ODW_CLIENT_SECRET` to your service principal secret

### "Could not resolve GUID for azure_datalake_gen2_resource_set"

→ The test asset may not exist in Purview. The default asset is: `service-user` entity in ServiceBus folder→ Update `PURVIEW_TEST_ASSET_QUALIFIED_NAME` to point to a different valid asset if needed