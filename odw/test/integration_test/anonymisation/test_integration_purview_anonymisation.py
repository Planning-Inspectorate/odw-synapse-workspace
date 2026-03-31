import os
import re
import pytest
import pyspark.sql.functions as F

from odw.core.anonymisation import AnonymisationEngine, fetch_purview_classifications_by_qualified_name
from odw.test.util.session_util import PytestSparkSessionUtil


@pytest.mark.integration
@pytest.mark.purview
class TestPurviewAnonymisationIntegration:
    @pytest.fixture(scope="class")
    def spark(self):
        return PytestSparkSessionUtil().get_spark_session()

    @pytest.fixture(scope="class")
    def purview_credentials(self):
        purview_name = os.getenv("ODW_PURVIEW_NAME", "pins-pview")
        tenant_id = os.getenv("ODW_TENANT_ID")
        client_id = os.getenv("ODW_CLIENT_ID")
        client_secret = os.getenv("ODW_CLIENT_SECRET", "AZURE_IDENTITY")

        if not tenant_id or not client_id:
            pytest.skip(
                "Purview integration tests require ODW_TENANT_ID and ODW_CLIENT_ID. "
                "Set ODW_CLIENT_SECRET or configure DefaultAzureCredential."
            )

        return {
            "purview_name": purview_name,
            "tenant_id": tenant_id,
            "client_id": client_id,
            "client_secret": client_secret,
        }

    @pytest.fixture(scope="class")
    def service_bus_test_asset_config(self):
        storage_host = os.getenv("ODW_STORAGE_ACCOUNT_DFS_HOST")
        if not storage_host:
            storage_host = os.getenv("DATA_LAKE_STORAGE_HOST", "pinsstodwdevuks9h80mb.dfs.core.windows.net")

        if not storage_host.endswith(".dfs.core.windows.net"):
            storage_host = f"{storage_host}.dfs.core.windows.net"

        asset_qn = os.getenv("PURVIEW_TEST_ASSET_QUALIFIED_NAME")
        if not asset_qn or "storage.dfs.core.windows.net/container/path/file.csv" in asset_qn:
            asset_qn = (
                f"https://{storage_host}/odw-raw/ServiceBus/service-user/"
                f"{{Year}}-{{Month}}-{{Day}}/"
                f"service-user_{{Year}}-{{Month}}-{{Day}}T{{Hour}}:{{N}}:{{N}}.{{N}}+{{N}}:{{N}}.json"
            )

        asset_type = os.getenv("PURVIEW_TEST_ASSET_TYPE", "azure_datalake_gen2_resource_set")

        return {
            "asset_qualified_name": asset_qn,
            "asset_type_name": asset_type,
            "storage_host": storage_host,
        }

    def _fetch_live_classifications(self, purview_credentials, service_bus_test_asset_config):
        return fetch_purview_classifications_by_qualified_name(
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=service_bus_test_asset_config["asset_type_name"],
            asset_qualified_name=service_bus_test_asset_config["asset_qualified_name"],
        )

    def _build_test_df(self, spark):
        data = [
            {
                "EmployeeID": "EMP001",
                "emailAddress": "john.doe@example.com",
                "full_name": "John Doe",
                "Age": 34,
                "NINumber": "AA000000A",
                "BirthDate": "1990-05-01",
                "AnnualSalary": 55000,
                "department": "Planning",
            },
            {
                "EmployeeID": "EMP002",
                "emailAddress": "jane.smith@example.com",
                "full_name": "Jane Smith",
                "Age": 45,
                "NINumber": "BB111111B",
                "BirthDate": "1980-08-15",
                "AnnualSalary": 72000,
                "department": "Operations",
            },
            {
                "EmployeeID": "EMP003",
                "emailAddress": None,
                "full_name": None,
                "Age": None,
                "NINumber": None,
                "BirthDate": None,
                "AnnualSalary": None,
                "department": "Technology",
            },
        ]
        return spark.createDataFrame(data)

    def _normalise(self, value):
        return re.sub(r"[^a-z0-9]", "", (value or "").lower())

    def _map_live_columns_to_df_columns(self, df, cols):
        norm_to_actual = {self._normalise(c): c for c in df.columns}
        mapping = {}
        for item in cols or []:
            live_col = item.get("column_name")
            if not live_col:
                continue
            actual_col = norm_to_actual.get(self._normalise(live_col))
            if actual_col:
                mapping[actual_col] = set(item.get("classifications") or [])
        return mapping

    def _is_supported_by_strategy(self, classifications):
        supported = {
            "MICROSOFT.PERSONAL.EMAIL",
            "Email Address",
            "Email Address Column Name",
            "MICROSOFT.PERSONAL.NAME",
            "First Name",
            "Last Name",
            "Names Column Name",
            "Birth Date",
            "Date of Birth",
            "Person's Age",
            "Employee Age",
            "NI Number",
            "Annual Salary",
        }
        return bool(set(classifications).intersection(supported))

    def test_can_fetch_classifications_from_purview_for_configured_fqn(
        self,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        cols = self._fetch_live_classifications(purview_credentials, service_bus_test_asset_config)

        assert isinstance(cols, list)

        for col_info in cols:
            assert "column_name" in col_info
            assert "classifications" in col_info
            assert isinstance(col_info["classifications"], list)

    def test_apply_from_purview_preserves_schema_and_row_count(
        self,
        spark,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        df = self._build_test_df(spark)

        engine = AnonymisationEngine(run_id="integration-live")
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=service_bus_test_asset_config["asset_type_name"],
            asset_qualified_name=service_bus_test_asset_config["asset_qualified_name"],
        )

        assert result_df.count() == df.count()
        assert set(result_df.columns) == set(df.columns)
        assert not any(col.startswith("__orig__") for col in result_df.columns)

    def test_apply_from_purview_changes_only_classified_supported_columns(
        self,
        spark,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        df = self._build_test_df(spark)
        live_cols = self._fetch_live_classifications(purview_credentials, service_bus_test_asset_config)
        live_mapping = self._map_live_columns_to_df_columns(df, live_cols)

        engine = AnonymisationEngine(run_id="integration-live-contract")
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=service_bus_test_asset_config["asset_type_name"],
            asset_qualified_name=service_bus_test_asset_config["asset_qualified_name"],
        )

        original_rows = [r.asDict() for r in df.collect()]
        result_rows = [r.asDict() for r in result_df.collect()]

        for original, result in zip(original_rows, result_rows):
            for col_name in df.columns:
                classifications = live_mapping.get(col_name, set())
                should_change = (
                    original[col_name] is not None
                    and len(classifications) > 0
                    and self._is_supported_by_strategy(classifications)
                )

                if should_change:
                    assert original[col_name] != result[col_name], (
                        f"Expected '{col_name}' to change. "
                        f"Classifications: {sorted(classifications)}"
                    )
                else:
                    assert original[col_name] == result[col_name], (
                        f"Expected '{col_name}' to stay unchanged. "
                        f"Classifications: {sorted(classifications)}"
                    )

    def test_apply_from_purview_preserves_null_values(
        self,
        spark,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        df = self._build_test_df(spark)

        engine = AnonymisationEngine(run_id="integration-live-nulls")
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=service_bus_test_asset_config["asset_type_name"],
            asset_qualified_name=service_bus_test_asset_config["asset_qualified_name"],
        )

        null_row = result_df.filter(F.col("EmployeeID") == "EMP003").collect()[0].asDict()

        assert null_row["emailAddress"] is None
        assert null_row["full_name"] is None
        assert null_row["Age"] is None
        assert null_row["NINumber"] is None
        assert null_row["BirthDate"] is None
        assert null_row["AnnualSalary"] is None

    def test_apply_from_purview_honours_classification_allowlist(
        self,
        spark,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        df = self._build_test_df(spark)
        live_cols = self._fetch_live_classifications(purview_credentials, service_bus_test_asset_config)
        live_mapping = self._map_live_columns_to_df_columns(df, live_cols)

        engine = AnonymisationEngine(run_id="integration-live-allowlist")
        result_df = engine.apply_from_purview(
            df,
            purview_name=purview_credentials["purview_name"],
            tenant_id=purview_credentials["tenant_id"],
            client_id=purview_credentials["client_id"],
            client_secret=purview_credentials["client_secret"],
            asset_type_name=service_bus_test_asset_config["asset_type_name"],
            asset_qualified_name=service_bus_test_asset_config["asset_qualified_name"],
            classification_allowlist=["MICROSOFT.PERSONAL.EMAIL"],
        )

        original_rows = [r.asDict() for r in df.collect()]
        result_rows = [r.asDict() for r in result_df.collect()]

        for original, result in zip(original_rows, result_rows):
            for col_name in df.columns:
                classifications = live_mapping.get(col_name, set())
                email_allowed = "MICROSOFT.PERSONAL.EMAIL" in classifications

                if original[col_name] is not None and email_allowed:
                    assert original[col_name] != result[col_name]
                else:
                    assert original[col_name] == result[col_name]

    def test_apply_from_purview_simplified_api_for_service_bus(
        self,
        spark,
        purview_credentials,
        service_bus_test_asset_config,
    ):
        df = self._build_test_df(spark)

        os.environ["ODW_PURVIEW_NAME"] = purview_credentials["purview_name"]
        os.environ["ODW_TENANT_ID"] = purview_credentials["tenant_id"]
        os.environ["ODW_CLIENT_ID"] = purview_credentials["client_id"]
        os.environ["ODW_CLIENT_SECRET"] = purview_credentials["client_secret"]
        os.environ["ODW_STORAGE_ACCOUNT_DFS_HOST"] = service_bus_test_asset_config["storage_host"]

        try:
            engine = AnonymisationEngine(run_id="integration-live-simplified")
            result_df = engine.apply_from_purview(
                df,
                entity_name="service-user",
                source_folder="ServiceBus",
            )

            assert result_df.count() == df.count()
            assert set(result_df.columns) == set(df.columns)
            assert not any(col.startswith("__orig__") for col in result_df.columns)
        finally:
            for key in [
                "ODW_PURVIEW_NAME",
                "ODW_TENANT_ID",
                "ODW_CLIENT_ID",
                "ODW_CLIENT_SECRET",
                "ODW_STORAGE_ACCOUNT_DFS_HOST",
            ]:
                os.environ.pop(key, None)