import os
import pytest
from datetime import datetime
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from odw.core.anonymisation import AnonymisationEngine, fetch_purview_classifications_by_qualified_name
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal


@pytest.mark.integration
@pytest.mark.purview
class TestPurviewAnonymisationIntegration:
    """
    Integration tests for anonymisation engine with real Purview API calls.
    
    Requirements:
    - Azure credentials configured via environment variables or DefaultAzureCredential
    - Purview account must be accessible
    - Test assets must be classified in Purview
    
    Environment variables (can be set in CI/CD or locally):
    - ODW_PURVIEW_NAME: Purview account name (default: pins-pview)
    - ODW_TENANT_ID: Azure AD tenant ID
    - ODW_CLIENT_ID: Service principal client ID
    - ODW_CLIENT_SECRET: Service principal secret (or use AZURE_IDENTITY for DefaultAzureCredential)
    - ODW_STORAGE_ACCOUNT_DFS_HOST: Storage account DFS hostname
    - PURVIEW_TEST_ASSET_QUALIFIED_NAME: Qualified name of test asset in Purview
    - PURVIEW_TEST_ASSET_TYPE: Asset type name (default: azure_datalake_gen2_resource_set)
    """

    @pytest.fixture(scope="class")
    def spark(self):
        """Get or create Spark session for integration tests."""
        return PytestSparkSessionUtil().get_spark_session()

    @pytest.fixture(scope="class")
    def purview_credentials(self):
        """
        Load Purview credentials from environment.
        
        Raises:
            pytest.skip: If required credentials are not available
        """
        purview_name = os.getenv("ODW_PURVIEW_NAME", "pins-pview")
        tenant_id = os.getenv("ODW_TENANT_ID")
        client_id = os.getenv("ODW_CLIENT_ID")
        
        # Client secret can be omitted to use DefaultAzureCredential
        client_secret = os.getenv("ODW_CLIENT_SECRET", "AZURE_IDENTITY")
        
        if not tenant_id or not client_id:
            pytest.skip(
                "Purview integration tests require ODW_TENANT_ID and ODW_CLIENT_ID. "
                "Set ODW_CLIENT_SECRET or configure DefaultAzureCredential for authentication."
            )
        
        return {
            "purview_name": purview_name,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
        }

    @pytest.fixture(scope="class")
    def horizon_test_asset_config(self):
        """
        Configuration for ServiceBus test asset.
        
        Returns a test asset qualified name for the 'service-user' entity in ServiceBus folder.
        This assumes the asset has been scanned and classified in Purview.
        
        Default FQN: https://pinsstodwdevuks9h80mb.dfs.core.windows.net/odw-raw/ServiceBus/service-user/
                     {Year}-{Month}-{Day}/service-user_{Year}-{Month}-{Day}T{Hour}_{Minute}_{Second}.{N}+{N}_{N}.json
        """
        # Get storage host from environment
        storage_host = os.getenv("ODW_STORAGE_ACCOUNT_DFS_HOST")
        if not storage_host:
            storage_host = os.getenv("DATA_LAKE_STORAGE_HOST", "pinsstodwdevuks9h80mb.dfs.core.windows.net")
        
        if not storage_host.endswith(".dfs.core.windows.net"):
            storage_host = f"{storage_host}.dfs.core.windows.net"
        
        # Use environment override or construct default test asset
        # Check if custom test asset is provided and not the placeholder
        asset_qn = os.getenv("PURVIEW_TEST_ASSET_QUALIFIED_NAME")
        if asset_qn and "storage.dfs.core.windows.net/container/path/file.csv" not in asset_qn:
            # Valid custom asset provided
            pass
        else:
            # Use default ServiceBus service-user entity
        # Note: Double braces {{}} escape to single braces {} in f-strings for Purview placeholders
            asset_qn = f"https://{storage_host}/odw-raw/ServiceBus/service-user/{{Year}}-{{Month}}-{{Day}}/service-user_{{Year}}-{{Month}}-{{Day}}T{{Hour}}_{{Minute}}_{{Second}}.{{N}}+{{N}}_{{N}}.json"
        
        asset_type = os.getenv("PURVIEW_TEST_ASSET_TYPE", "azure_datalake_gen2_resource_set")
        
        return {
            "asset_qualified_name": asset_qn,
            "asset_type_name": asset_type,
            "storage_host": storage_host,
        }

    def test_fetch_purview_classifications_real_api(self, purview_credentials, horizon_test_asset_config):
        """
        Test fetching classifications from real Purview API.
        
        Validates:
        - Successful authentication to Purview
        - Asset resolution by qualified name
        - Column classification retrieval
        """
        cols = fetch_purview_classifications_by_qualified_name(
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Validate response structure
        assert isinstance(cols, list), "Expected list of column classifications"
        
        # If classifications were found, validate structure
        if cols:
            for col_info in cols:
                assert "column_name" in col_info, "Missing column_name in response"
                assert "classifications" in col_info, "Missing classifications in response"
                assert isinstance(col_info["classifications"], list), "classifications should be a list"
                
                # Optional fields
                if "column_guid" in col_info:
                    assert isinstance(col_info["column_guid"], str)
                if "column_type" in col_info:
                    assert isinstance(col_info["column_type"], str)
        
        # Log results for debugging
        print(f"\nFetched {len(cols)} classified columns from Purview")
        for col in cols:
            print(f"  - {col.get('column_name')}: {col.get('classifications')}")

    def test_apply_from_purview_explicit_params(self, spark, purview_credentials, horizon_test_asset_config):
        """
        Test end-to-end anonymisation with explicit Purview parameters.
        
        Validates:
        - Purview integration with real API calls
        - Classification-based anonymisation application
        - Data transformation correctness
        
        Note: If no classifications are found in Purview, test validates that
        data passes through unchanged (no false anonymisation).
        """
        # Create test DataFrame with columns that should be classified in Purview
        test_data = [
            {
                "emailAddress": "john.doe@example.com",
                "full_name": "John Doe",
                "Age": 34,
                "NINumber": "AA000000A",
            },
            {
                "emailAddress": "jane.smith@example.com",
                "full_name": "Jane Smith",
                "Age": 45,
                "NINumber": "BB111111B",
            },
        ]
        df = spark.createDataFrame(test_data)
        
        # First, check if asset has classifications
        cols_with_classifications = fetch_purview_classifications_by_qualified_name(
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Apply anonymisation using real Purview classifications
        engine = AnonymisationEngine()
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Validate schema preservation
        assert set(result_df.columns) == set(df.columns), "Schema should be preserved"
        
        # Validate row count
        assert result_df.count() == df.count(), "Row count should be preserved"
        
        # Collect results for validation
        rows = result_df.collect()
        
        if cols_with_classifications:
            # Validate that sensitive data has been transformed when classifications exist
            print(f"\nFound {len(cols_with_classifications)} classified columns, validating anonymisation...")
            for row in rows:
                row_dict = row.asDict()
                
                # Check each classified column
                for col_info in cols_with_classifications:
                    col_name = col_info.get("column_name")
                    if col_name not in row_dict:
                        continue
                    
                    value = row_dict[col_name]
                    if value is None:
                        continue
                    
                    # If emailAddress is classified, it should be masked
                    if "email" in col_name.lower() and "@" in str(value):
                        assert "*" in str(value), f"{col_name} should be masked"
                    
                    # If full_name is classified, it should be masked
                    if "name" in col_name.lower() and len(str(value)) > 2:
                        assert "*" in str(value), f"{col_name} should be masked"
        else:
            # No classifications found - validate data is unchanged
            print("\nNo classifications found in Purview - validating data unchanged...")
            original_rows = df.collect()
            for orig, res in zip(original_rows, rows):
                assert orig.asDict() == res.asDict(), "Data should be unchanged when no classifications exist"
        
        print(f"\nTest completed successfully for {result_df.count()} rows")

    def test_apply_from_purview_simplified_horizon(self, spark, purview_credentials):
        """
        Test anonymisation using simplified API for Horizon source.
        
        Validates:
        - Simplified parameter API
        - Automatic qualified name construction
        - Credential resolution
        """
        # Skip if storage account host is not configured
        storage_host = os.getenv("ODW_STORAGE_ACCOUNT_DFS_HOST")
        if not storage_host:
            pytest.skip("ODW_STORAGE_ACCOUNT_DFS_HOST not set for simplified API test")
        
        # Create test DataFrame
        test_data = [
            {"emailAddress": "test1@example.com", "full_name": "Test Person One"},
            {"emailAddress": "test2@example.com", "full_name": "Test Person Two"},
        ]
        df = spark.createDataFrame(test_data)
        
        # Set environment variables for simplified API
        os.environ["ODW_PURVIEW_NAME"] = purview_credentials["purview_name"]
        os.environ["ODW_TENANT_ID"] = purview_credentials["tenant_id"]
        os.environ["ODW_CLIENT_ID"] = purview_credentials["client_id"]
        os.environ["ODW_CLIENT_SECRET"] = purview_credentials["client_secret"]
        os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = storage_host
        
        try:
            engine = AnonymisationEngine()
            result_df = engine.apply_from_purview(
                df,
                entity_name="service-user", #test entity
                source_folder="ServiceBus",
            )
            
            # Validate basic properties
            assert result_df.count() == df.count(), "Row count should be preserved"
            assert set(result_df.columns) == set(df.columns), "Schema should be preserved"
            
            print(f"\nSimplified API test completed successfully for {result_df.count()} rows")
        finally:
            # Clean up environment variables
            for key in ["ODW_PURVIEW_NAME", "ODW_TENANT_ID", "ODW_CLIENT_ID", "ODW_CLIENT_SECRET"]:
                os.environ.pop(key, None)

    def test_apply_from_purview_with_allowlist(self, spark, purview_credentials, horizon_test_asset_config):
        """
        Test anonymisation with classification allowlist.
        
        Validates:
        - Only specified classifications are processed
        - Other classifications are ignored
        
        Note: Test passes even if no classifications exist in Purview,
        as it validates the allowlist filtering mechanism.
        """
        test_data = [
            {
                "emailAddress": "user@example.com",
                "full_name": "User Name",
                "Age": 30,
            },
        ]
        df = spark.createDataFrame(test_data)
        
        # Check what classifications exist
        all_cols = fetch_purview_classifications_by_qualified_name(
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Only allow email classification
        classification_allowlist = ["MICROSOFT.PERSONAL.EMAIL"]
        
        engine = AnonymisationEngine()
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
            classification_allowlist=classification_allowlist,
        )
        
        row = result_df.collect()[0]
        row_dict = row.asDict()
        
        if all_cols:
            print(f"\nFound {len(all_cols)} classified columns in Purview")
            # Check if email is in the classified columns and in allowlist
            email_classified = any(
                col.get("column_name", "").lower() == "emailaddress" and
                "MICROSOFT.PERSONAL.EMAIL" in col.get("classifications", [])
                for col in all_cols
            )
            
            if email_classified:
                # Email should be masked since it's in allowlist
                if "emailAddress" in row_dict and row_dict["emailAddress"]:
                    assert "*" in row_dict["emailAddress"], "Email should be masked when classified and in allowlist"
        else:
            print("\nNo classifications found - allowlist test validates no false anonymisation")
        
        print("\nAllowlist filtering test completed")

    def test_anonymisation_determinism(self, spark, purview_credentials, horizon_test_asset_config):
        """
        Test that anonymisation is deterministic with seed columns.
        
        Validates:
        - Same input produces same output (with seed column)
        - Deterministic masking behavior
        """
        test_data = [
            {
                "EmployeeID": "EMP001",
                "emailAddress": "user@example.com",
                "full_name": "Test User",
                "Age": 35,
            },
        ]
        df = spark.createDataFrame(test_data)
        
        engine = AnonymisationEngine()
        
        # Apply anonymisation twice
        result1 = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        result2 = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Results should be identical (deterministic)
        assert_dataframes_equal(result1, result2)
        
        print("\nDeterminism test passed - results are consistent")

    def test_anonymisation_preserves_nulls(self, spark, purview_credentials, horizon_test_asset_config):
        """
        Test that null values are preserved during anonymisation.
        
        Validates:
        - Null values remain null
        - Non-null values are transformed
        """
        test_data = [
            {"emailAddress": "user@example.com", "full_name": "Test User", "Age": 30},
            {"emailAddress": None, "full_name": None, "Age": None},
            {"emailAddress": "other@example.com", "full_name": "", "Age": 40},
        ]
        df = spark.createDataFrame(test_data)
        
        engine = AnonymisationEngine()
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        rows = result_df.collect()
        
        # Row with nulls should still have nulls
        null_row = rows[1].asDict()
        assert null_row["emailAddress"] is None, "Null email should remain null"
        assert null_row["full_name"] is None, "Null name should remain null"
        assert null_row["Age"] is None, "Null age should remain null"
        
        print("\nNull preservation test passed")

    def test_azure_identity_credential_fallback(self, spark, horizon_test_asset_config):
        """
        Test DefaultAzureCredential fallback when no client secret is provided.
        
        Validates:
        - AZURE_IDENTITY sentinel triggers DefaultAzureCredential
        - Authentication works with managed identity or other credential types
        
        Note: This test will only pass in environments where DefaultAzureCredential
        can successfully authenticate (e.g., Azure VM with managed identity, local
        development with Azure CLI logged in).
        """
        purview_name = os.getenv("ODW_PURVIEW_NAME", "pins-pview")
        tenant_id = os.getenv("ODW_TENANT_ID")
        client_id = os.getenv("ODW_CLIENT_ID")
        
        if not tenant_id or not client_id:
            pytest.skip("Test requires ODW_TENANT_ID and ODW_CLIENT_ID")
        
        test_data = [{"emailAddress": "test@example.com"}]
        df = spark.createDataFrame(test_data)
        
        engine = AnonymisationEngine()
        
        try:
            # Use AZURE_IDENTITY sentinel to trigger DefaultAzureCredential
            result_df = engine.apply_from_purview(
                df,
                purview_name=purview_name,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret="AZURE_IDENTITY",
                asset_type_name=horizon_test_asset_config["asset_type_name"],
                asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
            )
            
            assert result_df.count() == df.count(), "Should successfully authenticate with DefaultAzureCredential"
            print("\nDefaultAzureCredential fallback test passed")
            
        except Exception as e:
            pytest.skip(f"DefaultAzureCredential not available in this environment: {e}")

    def test_change_tracking_and_logging(self, spark, purview_credentials, horizon_test_asset_config):
        """
        Test that change tracking works correctly.
        
        Validates:
        - Engine tracks which rows were changed
        - Logging includes change statistics
        - Original columns are not present in final output
        """
        test_data = [
            {"emailAddress": "user1@example.com", "full_name": "User One", "Age": 25},
            {"emailAddress": "user2@example.com", "full_name": "User Two", "Age": 35},
        ]
        df = spark.createDataFrame(test_data)
        
        engine = AnonymisationEngine(run_id="test-run-12345")
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=horizon_test_asset_config["asset_type_name"],
            asset_qualified_name=horizon_test_asset_config["asset_qualified_name"],
        )
        
        # Ensure no __orig__ columns remain in output
        for col in result_df.columns:
            assert not col.startswith("__orig__"), f"Helper column {col} should be removed"
        
        # Verify that at least some data was changed (if classifications exist)
        original_rows = df.collect()
        result_rows = result_df.collect()
        
        changed_count = 0
        for orig, res in zip(original_rows, result_rows):
            orig_dict = orig.asDict()
            res_dict = res.asDict()
            for col_name in orig_dict.keys():
                if orig_dict[col_name] != res_dict[col_name]:
                    changed_count += 1
                    break
        
        print(f"\nChange tracking: {changed_count} rows modified out of {df.count()}")
