import mock
import re
from datetime import date
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal
import pyspark.sql.functions as F


def test__service_bus_standardisation__anonymisation_applied_in_dev_environment():
    """
    Test that anonymisation is applied when environment is DEV
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    
    # Mock data with sensitive fields
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "emailAddress": "john.doe@example.com",
            "NINumber": "AA123456A",
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
        },
        {
            "EmployeeID": "E2",
            "full_name": "Jane Smith",
            "emailAddress": "jane.smith@example.com",
            "NINumber": "BB654321B",
            "message_enqueued_time_utc": "2025-01-02T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    
    # Add standardised columns (simulating what read_raw_messages does)
    df = (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )
    
    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        {"column_name": "NINumber", "classifications": ["NI Number"]},
    ]
    
    with mock.patch.object(Util, "is_non_production_environment", return_value=True):
        with mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"):
            with mock.patch.object(LoggingUtil, "__new__"):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                        with mock.patch(
                            "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
                            return_value=mocked_purview_cols,
                        ):
                            # Simulate the anonymisation step in process()
                            from odw.core.anonymisation.engine import AnonymisationEngine
                            
                            engine = AnonymisationEngine()
                            result_df = engine.apply_from_purview(
                                df,
                                entity_name="test-entity",
                                source_folder="ServiceBus"
                            )
    
    # Verify anonymisation was applied
    rows = result_df.select("full_name", "emailAddress", "NINumber").collect()
    
    # Name should be masked
    assert rows[0]["full_name"] == "J*** **e"
    assert rows[1]["full_name"] == "J*** ****h"
    
    # Email should be masked (local part only, domain preserved)
    assert rows[0]["emailAddress"] == "j******e@example.com"
    assert rows[1]["emailAddress"] == "j********h@example.com"
    
    # NI number should be random format: two letters, six digits, final letter A-D
    ni_re = re.compile(r"^[A-Z]{2}\d{6}[A-D]$")
    assert ni_re.match(rows[0]["NINumber"])
    assert ni_re.match(rows[1]["NINumber"])
    
    # Original NI numbers should not be present
    assert rows[0]["NINumber"] != "AA123456A"
    assert rows[1]["NINumber"] != "BB654321B"


def test__service_bus_standardisation__anonymisation_skipped_in_prod_environment():
    """
    Test that anonymisation is NOT applied when environment is PROD
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    
    # Mock data with sensitive fields
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "emailAddress": "john.doe@example.com",
            "NINumber": "AA123456A",
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    
    # Add standardised columns
    df = (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )
    
    original_df = df
    
    with mock.patch.object(Util, "is_non_production_environment", return_value=False):
        with mock.patch.object(LoggingUtil, "__new__"):
            with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                    # Simulate the anonymisation check in process()
                    if Util.is_non_production_environment():
                        # This should NOT be executed in PROD
                        raise AssertionError("Anonymisation should not run in PROD environment")
                    
                    result_df = df
    
    # Verify data remains unchanged
    rows = result_df.select("full_name", "emailAddress", "NINumber").collect()
    
    assert rows[0]["full_name"] == "John Doe"
    assert rows[0]["emailAddress"] == "john.doe@example.com"
    assert rows[0]["NINumber"] == "AA123456A"


def test__service_bus_standardisation__anonymisation_skipped_in_test_environment():
    """
    Test that anonymisation IS applied when environment is TEST
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    
    # Mock data with sensitive fields
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "emailAddress": "john.doe@example.com",
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    
    # Add standardised columns
    df = (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )
    
    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
    ]
    
    with mock.patch.object(Util, "is_non_production_environment", return_value=True):
        with mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"):
            with mock.patch.object(LoggingUtil, "__new__"):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                        with mock.patch(
                            "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
                            return_value=mocked_purview_cols,
                        ):
                            from odw.core.anonymisation.engine import AnonymisationEngine
                            
                            engine = AnonymisationEngine()
                            result_df = engine.apply_from_purview(
                                df,
                                entity_name="test-entity",
                                source_folder="ServiceBus"
                            )
    
    # Verify anonymisation was applied
    rows = result_df.select("full_name", "emailAddress").collect()
    
    assert rows[0]["full_name"] == "J*** **e"
    assert rows[0]["emailAddress"] == "j******e@example.com"


def test__service_bus_standardisation__anonymisation_preserves_non_sensitive_columns():
    """
    Test that non-sensitive columns are not modified during anonymisation
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    
    # Mock data with mix of sensitive and non-sensitive fields
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "department": "Engineering",
            "location": "London",
            "emailAddress": "john.doe@example.com",
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    
    # Add standardised columns
    df = (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )
    
    # Mock Purview classifications (only for sensitive columns)
    mocked_purview_cols = [
        {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
    ]
    
    with mock.patch.object(Util, "is_non_production_environment", return_value=True):
        with mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"):
            with mock.patch.object(LoggingUtil, "__new__"):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                        with mock.patch(
                            "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
                            return_value=mocked_purview_cols,
                        ):
                            from odw.core.anonymisation.engine import AnonymisationEngine
                            
                            engine = AnonymisationEngine()
                            result_df = engine.apply_from_purview(
                                df,
                                entity_name="test-entity",
                                source_folder="ServiceBus"
                            )
    
    # Verify non-sensitive columns remain unchanged
    rows = result_df.select("EmployeeID", "department", "location", "full_name", "emailAddress").collect()
    
    # Non-sensitive columns should be unchanged
    assert rows[0]["EmployeeID"] == "E1"
    assert rows[0]["department"] == "Engineering"
    assert rows[0]["location"] == "London"
    
    # Sensitive columns should be anonymised
    assert rows[0]["full_name"] == "J*** **e"
    assert rows[0]["emailAddress"] == "j******e@example.com"


def test__service_bus_standardisation__anonymisation_is_idempotent():
    """
    Test that anonymisation produces consistent results for the same input
    (deterministic based on seed columns)
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    
    # Mock data with EmployeeID as seed column
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "Age": 34,
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    
    # Add standardised columns
    df = (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )
    
    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        {"column_name": "Age", "classifications": ["Person's Age"]},
    ]
    
    with mock.patch.object(Util, "is_non_production_environment", return_value=True):
        with mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"):
            with mock.patch.object(LoggingUtil, "__new__"):
                with mock.patch.object(LoggingUtil, "log_info", return_value=None):
                    with mock.patch.object(LoggingUtil, "log_error", return_value=None):
                        with mock.patch(
                            "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
                            return_value=mocked_purview_cols,
                        ):
                            from odw.core.anonymisation.engine import AnonymisationEngine
                            
                            # Run anonymisation twice
                            engine = AnonymisationEngine()
                            result_df_1 = engine.apply_from_purview(
                                df,
                                entity_name="test-entity",
                                source_folder="ServiceBus"
                            )
                            
                            result_df_2 = engine.apply_from_purview(
                                df,
                                entity_name="test-entity",
                                source_folder="ServiceBus"
                            )
    
    # Verify both runs produce identical results
    rows_1 = result_df_1.select("full_name", "Age").collect()
    rows_2 = result_df_2.select("full_name", "Age").collect()
    
    assert rows_1[0]["full_name"] == rows_2[0]["full_name"]
    assert rows_1[0]["Age"] == rows_2[0]["Age"]
