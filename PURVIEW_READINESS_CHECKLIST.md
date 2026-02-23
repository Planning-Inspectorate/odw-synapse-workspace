# Purview Readiness Checklist for Anonymisation

## Overview
Before implementing the anonymisation integration, ensure your Azure Purview instance is properly configured to provide column classifications for the anonymisation engine.

## Critical Requirements

### 1. Purview Assets Must Exist and Be Registered

The anonymisation engine looks for **ADLS Gen2 Resource Set** assets with specific qualified name patterns.

#### Service Bus Assets

For `appeal-has`, Purview must have an asset with this qualified name pattern:

```
https://<storage-account>.dfs.core.windows.net/odw-raw/ServiceBus/appeal-has/{Year}-{Month}-{Day}/appeal-has_{Year}-{Month}-{Day}T{Hour}:{N}:{N}.{N}+{N}:{N}.json
```

**Key Points:**
- Asset type: `azure_datalake_gen2_resource_set`
- Must use Purview pattern placeholders: `{Year}`, `{Month}`, `{Day}`, `{Hour}`, `{N}`
- Pattern must match your actual file structure in ADLS

#### Horizon Assets

For Horizon data:

```
https://<storage-account>.dfs.core.windows.net/odw-raw/Horizon/{Year}-{Month}-{Day}/<filename>.csv
```

### 2. Data Source Must Be Scanned

**Action Required:**

1. **Verify scan is configured:**
   - Go to Purview Studio → Data Map → Sources
   - Find your ADLS Gen2 storage account
   - Check that `odw-raw` container is included

2. **Ensure scans are running:**
   - Check scan history for recent successful runs
   - Scans should run regularly (daily/weekly)
   - Verify `ServiceBus/appeal-has` folder is being scanned

3. **Trigger manual scan if needed:**
   ```
   1. Navigate to your data source in Purview
   2. Click "New scan" or "Run scan"
   3. Select scope: odw-raw/ServiceBus/appeal-has
   4. Wait for completion (can take 15-30 minutes)
   ```

### 3. Classifications Must Be Applied

The anonymisation engine expects these **specific classification names**:

| Classification Name | Purpose | Where to Apply |
|---------------------|---------|----------------|
| `MICROSOFT.PERSONAL.NAME` | Full names | Name columns |
| `MICROSOFT.PERSONAL.EMAIL` | Email addresses | Email columns |
| `NI Number` | National Insurance | NI number columns |
| `Birth Date` | Date of birth | DOB columns |
| `Date of Birth` | Date of birth (alt) | DOB columns |
| `Person's Age` | Age values | Age columns |
| `Employee Age` | Employee age | Age columns |
| `Annual Salary` | Salary data | Salary columns |
| `First Name` | First names only | First name columns |
| `Last Name` | Last names only | Last name columns |
| `Names Column Name` | Generic name | Name columns |
| `Email Address` | Email (alt) | Email columns |
| `Email Address Column Name` | Email (alt) | Email columns |

**Classification Options:**

#### Option A: Automatic Classification (Recommended)

Purview can auto-classify using built-in classification rules:

1. **Enable classification rules in scan:**
   - Edit your scan configuration
   - Under "Classification rules", enable:
     - `MICROSOFT.PERSONAL.NAME`
     - `MICROSOFT.PERSONAL.EMAIL`
     - Any custom rules you've created

2. **Create custom classification rules for UK-specific data:**
   - In Purview Studio → Data Map → Classifications
   - Create custom classification: `NI Number`
     - Pattern: `[A-Z]{2}[0-9]{6}[A-D]`
     - Minimum match: 80%
   - Create: `Birth Date` (date format patterns)
   - Create: `Annual Salary` (numeric range patterns)

#### Option B: Manual Classification

If auto-classification doesn't work:

1. Navigate to the asset in Purview
2. View the schema/columns
3. Manually apply classifications to each column
4. Save changes

### 4. Verify Asset Accessibility

**Test Purview API Access:**

Create a test script to verify you can reach Purview and retrieve classifications:

```python
from odw.core.anonymisation import fetch_purview_classifications_by_qualified_name

# Your configuration
purview_name = "pins-pview"
tenant_id = "5878df98-6f88-48ab-9322-998ce557088d"
client_id = "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
client_secret = "<your-secret>"  # or "AZURE_IDENTITY"

# Expected qualified name for appeal-has
storage_host = "<your-storage>.dfs.core.windows.net"
qualified_name = (
    f"https://{storage_host}/odw-raw/ServiceBus/appeal-has/"
    "{Year}-{Month}-{Day}/"
    "appeal-has_{Year}-{Month}-{Day}T{Hour}:{N}:{N}.{N}+{N}:{N}.json"
)

try:
    columns = fetch_purview_classifications_by_qualified_name(
        purview_name=purview_name,
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        asset_type_name="azure_datalake_gen2_resource_set",
        asset_qualified_name=qualified_name
    )
    
    print(f"✅ Found {len(columns)} classified columns:")
    for col in columns:
        print(f"  - {col['column_name']}: {col['classifications']}")
        
except Exception as e:
    print(f"❌ Error: {e}")
    print("\nTroubleshooting steps:")
    print("1. Verify asset exists in Purview with exact qualified name")
    print("2. Check data source is scanned")
    print("3. Verify classifications are applied")
    print("4. Check service principal has permissions")
```

### 5. Service Principal Permissions

The service principal (Client ID) must have appropriate permissions:

**Required Purview Roles:**

- **Data Reader** on the Purview collection containing your assets
  - Allows reading asset metadata and classifications

**How to Verify/Grant:**

1. Go to Purview Studio → Data Map → Collections
2. Find the collection containing your ADLS data source
3. Click Role assignments
4. Verify your service principal has "Data Reader" role
5. If not, add it:
   - Click "+ Add"
   - Select "Data Reader"
   - Add your service principal (Client ID)

**Required Storage Permissions:**

The service principal also needs read access to the storage account (for the simplified API mode):

- **Storage Blob Data Reader** on the storage account or container

### 6. Verify Column Name Matching

The anonymisation engine **normalizes** column names for matching. Verify your columns will match:

**Normalization Rules:**
- Trim and lowercase
- Remove non-alphanumeric characters
- Fix typos (e.g., `adress` → `address`)

**Examples:**
- `first_name` = `FirstName` = `first name` = `firstname`
- `email_address` = `EmailAddress` = `email address`

**Test Your Columns:**

```python
import re

def normalize_column_name(s):
    raw = (s or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9]", "", raw)
    cleaned = cleaned.replace("adress", "address")
    return cleaned

# Test your column names
columns = ["appellant_name", "email_address", "ni_number", "date_of_birth"]
for col in columns:
    print(f"{col} → {normalize_column_name(col)}")
```

Ensure your Purview column names will match your DataFrame column names after normalization.

## Quick Verification Steps

### Step 1: Check Asset Exists

```bash
# Using Azure CLI (if available)
az purview account get --name pins-pview --resource-group <resource-group>

# Or use Purview Studio:
# 1. Go to https://web.purview.azure.com
# 2. Browse Data Catalog
# 3. Search for: "appeal-has"
# 4. Verify asset appears
```

### Step 2: Check Classifications

In Purview Studio:

1. Find your `appeal-has` asset
2. Click on it to view details
3. Navigate to "Schema" tab
4. Verify columns have classification labels
5. Check that classification names match expected values

**Expected View:**

```
Columns:
┌─────────────────────┬─────────────┬───────────────────────────────┐
│ Column Name         │ Type        │ Classifications               │
├─────────────────────┼─────────────┼───────────────────────────────┤
│ appellant_name      │ string      │ MICROSOFT.PERSONAL.NAME       │
│ email_address       │ string      │ MICROSOFT.PERSONAL.EMAIL      │
│ ni_number           │ string      │ NI Number                     │
│ date_of_birth       │ date        │ Birth Date                    │
│ age                 │ int         │ Person's Age                  │
│ case_reference      │ string      │ (none)                        │
└─────────────────────┴─────────────┴───────────────────────────────┘
```

### Step 3: Test Purview API Connection

Run the test script above (in section 4) to verify end-to-end connectivity.

## Common Issues and Solutions

### Issue 1: Asset Not Found

**Symptoms:**
```
Error: Could not resolve GUID for azure_datalake_gen2_resource_set
```

**Solutions:**
1. ✅ Verify data source is registered in Purview
2. ✅ Check that scan has completed successfully
3. ✅ Verify qualified name pattern exactly matches
4. ✅ Check for typos in entity name (e.g., `appeal-has` vs `appeal_has`)

### Issue 2: No Classifications Found

**Symptoms:**
```
Warning: No columns with classifications found
```

**Solutions:**
1. ✅ Run scan with classification rules enabled
2. ✅ Manually apply classifications if auto-classification failed
3. ✅ Wait for classification to propagate (can take a few minutes)
4. ✅ Check classification names match exactly (case-sensitive!)

### Issue 3: Permission Denied

**Symptoms:**
```
HTTP 403 - Forbidden
```

**Solutions:**
1. ✅ Grant "Data Reader" role to service principal
2. ✅ Verify service principal Client ID is correct
3. ✅ Check client secret is valid and not expired
4. ✅ Ensure MSI/Managed Identity has necessary roles

### Issue 4: Column Name Mismatch

**Symptoms:**
```
Log: Classified column 'AppellantName' not found in DataFrame
```

**Solutions:**
1. ✅ Verify column names in DataFrame vs Purview
2. ✅ Check normalization (see section 6)
3. ✅ Update Purview column names if needed
4. ✅ Or update DataFrame column names

## Minimal Viable Configuration

If you want to **test without full Purview setup**, you can use explicit classifications:

```python
from odw.core.anonymisation import AnonymisationEngine

engine = AnonymisationEngine()

# Manually specify classifications (bypass Purview)
columns_with_classifications = [
    {"column_name": "appellant_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
    {"column_name": "email_address", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
    {"column_name": "ni_number", "classifications": ["NI Number"]},
]

anonymised_df = engine.apply(df, columns_with_classifications)
```

This is useful for:
- Initial testing
- Development environments
- When Purview is not available

## Pre-Implementation Checklist

Before implementing anonymisation integration:

- [ ] **Purview account accessible** (`pins-pview`)
- [ ] **ADLS Gen2 data source registered** in Purview
- [ ] **Scan configured and completed** for `odw-raw/ServiceBus/appeal-has`
- [ ] **Classifications applied** to columns (auto or manual)
- [ ] **Service principal has "Data Reader" role** in Purview
- [ ] **Asset qualified name verified** (matches expected pattern)
- [ ] **API connectivity tested** (test script runs successfully)
- [ ] **Column names match** between Purview and DataFrame (after normalization)
- [ ] **Client credentials available** (Client ID, Tenant ID, Secret/MSI)
- [ ] **Storage account DFS host known** (for simplified API)

## Recommended Approach

### Phase 1: Manual Testing (1-2 days)

1. ✅ Verify Purview setup (asset exists, scanned, classified)
2. ✅ Test Purview API connection
3. ✅ Run anonymisation with explicit classifications (no Purview dependency)
4. ✅ Verify output is correctly masked

### Phase 2: Purview Integration (1 day)

1. ✅ Switch to Purview-driven mode
2. ✅ Test with appeal-has in debug mode
3. ✅ Verify classifications are automatically detected
4. ✅ Monitor logs for any issues

### Phase 3: Production Rollout (ongoing)

1. ✅ Enable for all entities
2. ✅ Set up alerting for Purview failures
3. ✅ Regular scan schedule maintenance
4. ✅ Classification rule refinement

## Support Resources

- **Purview Documentation**: https://learn.microsoft.com/azure/purview/
- **Classification Rules**: https://learn.microsoft.com/azure/purview/supported-classifications
- **Your Anonymisation Package**: `odw/core/anonymisation/README.md`
- **Test Script**: See section 4 above

## Summary

**What Purview Must Provide:**
1. ✅ Asset registration for your data files
2. ✅ Column-level classifications
3. ✅ API access for service principal

**What You Need to Configure:**
1. ✅ Data source scanning
2. ✅ Classification rules (auto or manual)
3. ✅ Service principal permissions
4. ✅ Asset qualified names

**Fallback Option:**
- Can use explicit classifications if Purview not ready
- Allows testing without full Purview setup
- Can migrate to Purview-driven mode later

Once these prerequisites are met, the anonymisation integration will work automatically! 🎉
