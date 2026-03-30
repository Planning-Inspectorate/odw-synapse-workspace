# Anonymisation Unit Tests: Testing Guide

## Overview
This document describes the unit tests created for the anonymisation integration in the Service Bus and Horizon standardisation processes.

## Test Files

### 1. Service Bus Anonymisation Tests
**File**: `odw/test/unit_test/etl/test_service_bus_standardisation_anonymisation.py`

Contains 5 comprehensive test cases covering:
- Environment-based gating (DEV/TEST vs PROD)
- Anonymisation application and verification
- Preservation of non-sensitive columns
- Idempotence (deterministic results)

### 2. Horizon Anonymisation Tests
**File**: `odw/test/unit_test/etl/test_horizon_standardisation_anonymisation.py`

Contains 6 comprehensive test cases covering:
- Environment-based gating (DEV/TEST vs PROD)
- Anonymisation application and verification
- Preservation of non-sensitive columns
- Column name transformation handling
- Idempotence (deterministic results)
- Null value handling

---

## Running the Tests

### Prerequisites
Ensure you have the test dependencies installed:
```bash
pip install pytest mock delta-spark
```

### Run All Anonymisation Tests
```bash
# From the repository root
pytest odw/test/unit_test/etl/test_service_bus_standardisation_anonymisation.py -v
pytest odw/test/unit_test/etl/test_horizon_standardisation_anonymisation.py -v
```

### Run Specific Test
```bash
# Run a specific test function
pytest odw/test/unit_test/etl/test_service_bus_standardisation_anonymisation.py::test__service_bus_standardisation__anonymisation_applied_in_dev_environment -v
```

### Run with Coverage
```bash
pytest odw/test/unit_test/etl/test_service_bus_standardisation_anonymisation.py --cov=odw.core.etl.transformation.standardised.service_bus_standardisation_process --cov-report=html
```

---

## Test Case Details

### Service Bus Tests

#### 1. `test__service_bus_standardisation__anonymisation_applied_in_dev_environment`
**Purpose**: Verify that anonymisation is applied when the environment is DEV

**Scenario**:
- Mock Service Bus messages with sensitive fields (full_name, emailAddress, NINumber)
- Mock `Util.is_non_production_environment()` to return `True`
- Mock Purview to return classifications for sensitive columns
- Apply anonymisation via `AnonymisationEngine.apply_from_purview()`

**Assertions**:
- Names are masked (e.g., "John Doe" → "J*** **e")
- Emails are masked (e.g., "john.doe@example.com" → "j******e@example.com")
- NI numbers are randomized (format: `[A-Z]{2}\d{6}[A-D]`)
- Original sensitive data is not present

---

#### 2. `test__service_bus_standardisation__anonymisation_skipped_in_prod_environment`
**Purpose**: Verify that anonymisation is NOT applied when the environment is PROD

**Scenario**:
- Mock Service Bus messages with sensitive fields
- Mock `Util.is_non_production_environment()` to return `False`
- Verify anonymisation block is not executed

**Assertions**:
- All data remains unchanged
- Original values are preserved

---

#### 3. `test__service_bus_standardisation__anonymisation_skipped_in_test_environment`
**Purpose**: Verify that anonymisation IS applied when the environment is TEST

**Scenario**:
- Mock Service Bus messages with sensitive fields
- Mock `Util.is_non_production_environment()` to return `True`
- Apply anonymisation

**Assertions**:
- Sensitive columns are anonymised (same as DEV)

---

#### 4. `test__service_bus_standardisation__anonymisation_preserves_non_sensitive_columns`
**Purpose**: Verify that non-sensitive columns are not modified

**Scenario**:
- Mock data with both sensitive (full_name, emailAddress) and non-sensitive (EmployeeID, department, location) columns
- Only classify sensitive columns in Purview mock
- Apply anonymisation

**Assertions**:
- Non-sensitive columns remain unchanged
- Sensitive columns are anonymised

---

#### 5. `test__service_bus_standardisation__anonymisation_is_idempotent`
**Purpose**: Verify that anonymisation produces deterministic results

**Scenario**:
- Mock data with a seed column (EmployeeID)
- Run anonymisation twice with identical input
- Compare results

**Assertions**:
- Both runs produce identical anonymised values

---

### Horizon Tests

#### 1. `test__horizon_standardisation__anonymisation_applied_in_dev_environment`
**Purpose**: Verify that anonymisation is applied to Horizon CSV data when environment is DEV

**Scenario**:
- Mock Horizon CSV with sensitive fields (First Name, Last Name, Email Address, Birth Date, Annual Salary)
- Mock environment as DEV
- Mock Purview classifications
- Apply anonymisation

**Assertions**:
- Names masked (e.g., "John" → "J***", "Doe" → "D**")
- Emails masked
- Birth dates within anonymised range (1955-01-01 to 2005-12-31)
- Salaries within anonymised range (20000 to 100000)

---

#### 2. `test__horizon_standardisation__anonymisation_skipped_in_prod_environment`
**Purpose**: Verify that anonymisation is NOT applied when environment is PROD

**Scenario**:
- Mock Horizon CSV with sensitive fields
- Mock environment as PROD
- Verify anonymisation is skipped

**Assertions**:
- All data remains unchanged

---

#### 3. `test__horizon_standardisation__anonymisation_preserves_non_sensitive_columns`
**Purpose**: Verify non-sensitive columns are preserved

**Scenario**:
- Mock data with sensitive and non-sensitive columns
- Classify only sensitive columns
- Apply anonymisation

**Assertions**:
- Non-sensitive columns (Staff Number, Department, Location) unchanged
- Sensitive columns anonymised

---

#### 4. `test__horizon_standardisation__anonymisation_handles_column_name_transformations`
**Purpose**: Verify anonymisation works after Horizon's column name transformations

**Scenario**:
- Mock CSV with special characters in column names (e.g., "Staff-Number", "Email@Address")
- Apply Horizon's column transformation (lowercase, replace special chars with underscore)
- Mock Purview to return normalized column names
- Apply anonymisation

**Assertions**:
- Anonymisation engine matches normalized column names
- Classified columns are anonymised
- Non-classified columns unchanged

---

#### 5. `test__horizon_standardisation__anonymisation_is_idempotent`
**Purpose**: Verify deterministic anonymisation based on seed columns

**Scenario**:
- Mock data with Staff Number as seed
- Run anonymisation twice
- Compare results

**Assertions**:
- Both runs produce identical results

---

#### 6. `test__horizon_standardisation__anonymisation_handles_null_values`
**Purpose**: Verify that null values are handled correctly

**Scenario**:
- Mock data with some null values in sensitive columns
- Apply anonymisation

**Assertions**:
- Non-null values are anonymised
- Null values remain null

---

## Mocking Strategy

All tests use the following mocking approach:

### 1. Environment Gating Mock
```python
mock.patch.object(Util, "is_non_production_environment", return_value=True/False)
```
Controls whether anonymisation runs based on environment.

### 2. Logging Mock
```python
mock.patch.object(LoggingUtil, "__new__")
mock.patch.object(LoggingUtil, "log_info", return_value=None)
mock.patch.object(LoggingUtil, "log_error", return_value=None)
```
Prevents logging side effects during tests.

### 3. Purview Classifications Mock
```python
mock.patch(
    "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
    return_value=mocked_purview_cols,
)
```
Simulates Purview API response with column classifications without requiring actual Purview access.

---

## Test Data Patterns

### Service Bus Test Data
```python
data = [
    {
        "EmployeeID": "E1",
        "full_name": "John Doe",
        "emailAddress": "john.doe@example.com",
        "NINumber": "AA123456A",
        "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
    },
]
```

### Horizon Test Data
```python
data = [
    {
        "Staff Number": "S001",
        "First Name": "John",
        "Last Name": "Doe",
        "Email Address": "john.doe@example.com",
        "Birth Date": "1990-01-01",
        "Annual Salary": 50000,
    },
]
```

### Purview Mock Data
```python
mocked_purview_cols = [
    {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
    {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
    {"column_name": "NINumber", "classifications": ["NI Number"]},
]
```

---

## Expected Anonymisation Results

| Field Type | Original | Anonymised | Strategy |
|------------|----------|------------|----------|
| Full Name | "John Doe" | "J*** **e" | Name masking: first letter of first name, last letter of last name |
| Email | "john.doe@example.com" | "j******e@example.com" | Email masking: local part masked, domain preserved |
| NI Number | "AA123456A" | Random (e.g., "XY654321C") | Random NI number format: `[A-Z]{2}\d{6}[A-D]` |
| Age | 34 | Random (18-70) | Age remapping |
| Birth Date | "1990-01-01" | Random date (1955-01-01 to 2005-12-31) | Date remapping |
| Salary | 50000 | Random (20000-100000) | Salary remapping |

---

## Troubleshooting

### Test Fails with "Module not found"
Ensure all dependencies are installed:
```bash
pip install -r requirements.txt
pip install pytest mock delta-spark
```

### Test Fails with SparkSession Error
The `PytestSparkSessionUtil` creates an isolated Spark session for testing. If you encounter issues:
1. Check that `delta-spark` is installed
2. Ensure no other Spark sessions are running
3. Try running tests sequentially: `pytest -n 1`

### Purview Mock Not Working
Ensure the mock path is correct:
```python
"odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name"
```
This must match the import path in the production code.

---

## Test Coverage

Current test coverage for anonymisation integration:

| Component | Coverage |
|-----------|----------|
| Environment detection (Util.get_environment) | ✅ Covered |
| Environment gating (Util.is_non_production_environment) | ✅ Covered |
| Service Bus anonymisation application | ✅ Covered |
| Horizon anonymisation application | ✅ Covered |
| Column name normalization | ✅ Covered |
| Null handling | ✅ Covered |
| Idempotence | ✅ Covered |
| Non-sensitive column preservation | ✅ Covered |

---

## Next Steps

### Running Tests in CI/CD
Add the following to your Azure DevOps pipeline:
```yaml
- task: Bash@3
  displayName: 'Run Anonymisation Unit Tests'
  inputs:
    targetType: 'inline'
    script: |
      pytest odw/test/unit_test/etl/test_service_bus_standardisation_anonymisation.py -v
      pytest odw/test/unit_test/etl/test_horizon_standardisation_anonymisation.py -v
```

### Integration Testing
Consider creating integration tests that:
1. Deploy to a TEST environment
2. Trigger actual standardisation pipelines
3. Verify anonymised data in Delta tables
4. Validate Purview integration end-to-end

---

## References

- [pytest Documentation](https://docs.pytest.org/)
- [Python mock Library](https://docs.python.org/3/library/unittest.mock.html)
- [PySpark Testing Best Practices](https://spark.apache.org/docs/latest/api/python/user_guide/testing.html)
- [IMPLEMENTATION_SUMMARY.md](./IMPLEMENTATION_SUMMARY.md)
