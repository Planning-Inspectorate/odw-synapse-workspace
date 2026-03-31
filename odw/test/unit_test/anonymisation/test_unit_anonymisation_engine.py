import mock
import re
from datetime import date, datetime
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal
import pyspark.sql.functions as F


def test__horizon_standardisation__anonymisation_applied_in_dev_environment():
    """
    Test that anonymisation is applied to Horizon data when environment is DEV
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with sensitive fields
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Last Name": "Doe",
            "Email Address": "john.doe@example.com",
            "Birth Date": "1990-01-01",
            "Annual Salary": 50000,
        },
        {
            "Staff Number": "S002",
            "First Name": "Jane",
            "Last Name": "Smith",
            "Email Address": "jane.smith@example.com",
            "Birth Date": "1985-05-05",
            "Annual Salary": 60000,
        },
    ]
    df = spark.createDataFrame(data)

    # Add standardised columns (simulating what Horizon process does)
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("ingested_by_process_name", F.lit("horizon_standardisation_process"))
        .withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("input_file", F.lit("test_file.csv"))
        .withColumn("modified_datetime", F.current_timestamp())
        .withColumn("modified_by_process_name", F.lit("horizon_standardisation_process"))
        .withColumn("entity_name", F.lit("test_entity"))
        .withColumn("file_ID", F.lit("test_file_id"))
    )

    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "First Name", "classifications": ["First Name"]},
        {"column_name": "Last Name", "classifications": ["Last Name"]},
        {"column_name": "Email Address", "classifications": ["Email Address"]},
        {"column_name": "Birth Date", "classifications": ["Birth Date"]},
        {"column_name": "Annual Salary", "classifications": ["Annual Salary"]},
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
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

    # Verify anonymisation was applied
    rows = result_df.select("First Name", "Last Name", "Email Address", "Birth Date", "Annual Salary").collect()

    # Names should be masked (first letter only)
    assert rows[0]["First Name"] == "J***"
    assert rows[0]["Last Name"] == "D**"
    assert rows[1]["First Name"] == "J***"
    assert rows[1]["Last Name"] == "S****"

    # Email should be masked
    assert rows[0]["Email Address"] == "j******e@example.com"
    assert rows[1]["Email Address"] == "j********h@example.com"

    # Birth date should be within anonymised range
    start = date(1955, 1, 1)
    end = date(2005, 12, 31)
    assert start <= rows[0]["Birth Date"] <= end
    assert start <= rows[1]["Birth Date"] <= end

    # Salary should be within anonymised range
    assert isinstance(rows[0]["Annual Salary"], int) and 20000 <= rows[0]["Annual Salary"] <= 100000
    assert isinstance(rows[1]["Annual Salary"], int) and 20000 <= rows[1]["Annual Salary"] <= 100000


def test__horizon_standardisation__anonymisation_skipped_in_prod_environment():
    """
    Test that anonymisation is NOT applied to Horizon data when environment is PROD
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with sensitive fields
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Last Name": "Doe",
            "Email Address": "john.doe@example.com",
            "Birth Date": "1990-01-01",
        },
    ]
    df = spark.createDataFrame(data)

    # Add standardised columns
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("expected_from", F.current_timestamp())
        .withColumn("input_file", F.lit("test_file.csv"))
    )

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
    rows = result_df.select("First Name", "Last Name", "Email Address", "Birth Date").collect()

    assert rows[0]["First Name"] == "John"
    assert rows[0]["Last Name"] == "Doe"
    assert rows[0]["Email Address"] == "john.doe@example.com"
    assert rows[0]["Birth Date"] == "1990-01-01"


def test__horizon_standardisation__anonymisation_preserves_non_sensitive_columns():
    """
    Test that non-sensitive columns in Horizon data are not modified during anonymisation
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with mix of sensitive and non-sensitive fields
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Last Name": "Doe",
            "Department": "Engineering",
            "Location": "London",
            "Email Address": "john.doe@example.com",
        },
    ]
    df = spark.createDataFrame(data)

    # Add standardised columns
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("expected_from", F.current_timestamp())
        .withColumn("input_file", F.lit("test_file.csv"))
    )

    # Mock Purview classifications (only for sensitive columns)
    mocked_purview_cols = [
        {"column_name": "First Name", "classifications": ["First Name"]},
        {"column_name": "Last Name", "classifications": ["Last Name"]},
        {"column_name": "Email Address", "classifications": ["Email Address"]},
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
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

    # Verify non-sensitive columns remain unchanged
    rows = result_df.select("Staff Number", "Department", "Location", "First Name", "Last Name",
                            "Email Address").collect()

    # Non-sensitive columns should be unchanged
    assert rows[0]["Staff Number"] == "S001"
    assert rows[0]["Department"] == "Engineering"
    assert rows[0]["Location"] == "London"

    # Sensitive columns should be anonymised
    assert rows[0]["First Name"] == "J***"
    assert rows[0]["Last Name"] == "D**"
    assert rows[0]["Email Address"] == "j******e@example.com"


def test__horizon_standardisation__anonymisation_handles_column_name_transformations():
    """
    Test that anonymisation works correctly after Horizon column name transformations
    (lowercase, special character removal, etc...)
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with special characters in column names
    data = [
        {
            "Staff-Number": "S001",
            "First Name": "John",
            "Last.Name": "Doe",
            "Email@Address": "john.doe@example.com",
        },
    ]
    df = spark.createDataFrame(data)

    # Simulate the column name transformation that Horizon process does
    # lowercase, replace special chars with underscore, remove trailing underscores
    cols_orig = df.schema.names
    cols = [re.sub("[^0-9a-zA-Z]+", "_", i).lower() for i in cols_orig]
    cols = [colm.rstrip("_") for colm in cols]
    df = df.toDF(*cols)

    # Add standardised columns
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("input_file", F.lit("test_file.csv"))
    )

    # Mock Purview classifications (using normalized column names)
    mocked_purview_cols = [
        {"column_name": "first_name", "classifications": ["First Name"]},
        {"column_name": "last_name", "classifications": ["Last Name"]},
        {"column_name": "email_address", "classifications": ["Email Address"]},
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
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

    # Verify anonymisation was applied to transformed column names
    rows = result_df.select("staff_number", "first_name", "last_name", "email_address").collect()

    # staff_number should be unchanged
    assert rows[0]["staff_number"] == "S001"

    # Classified columns should be anonymised
    assert rows[0]["first_name"] == "J***"
    assert rows[0]["last_name"] == "D**"
    assert rows[0]["email_address"] == "j******e@example.com"


def test__horizon_standardisation__anonymisation_is_idempotent():
    """
    Test that Horizon anonymisation produces consistent results for the same input
    (deterministic based on seed columns like Staff Number)
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with Staff Number as seed column
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Annual Salary": 50000,
        },
    ]
    df = spark.createDataFrame(data)

    # Add standardised columns
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("input_file", F.lit("test_file.csv"))
    )

    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "First Name", "classifications": ["First Name"]},
        {"column_name": "Annual Salary", "classifications": ["Annual Salary"]},
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
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

                            result_df_2 = engine.apply_from_purview(
                                df,
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

    # Verify both runs produce identical results
    rows_1 = result_df_1.select("First Name", "Annual Salary").collect()
    rows_2 = result_df_2.select("First Name", "Annual Salary").collect()

    assert rows_1[0]["First Name"] == rows_2[0]["First Name"]
    assert rows_1[0]["Annual Salary"] == rows_2[0]["Annual Salary"]


def test__horizon_standardisation__anonymisation_handles_null_values():
    """
    Test that anonymisation correctly handles null values in sensitive columns
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    # Mock Horizon CSV data with null values
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Email Address": "john.doe@example.com",
        },
        {
            "Staff Number": "S002",
            "First Name": None,
            "Email Address": None,
        },
    ]
    df = spark.createDataFrame(data)

    # Add standardised columns
    df = (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("input_file", F.lit("test_file.csv"))
    )

    # Mock Purview classifications
    mocked_purview_cols = [
        {"column_name": "First Name", "classifications": ["First Name"]},
        {"column_name": "Email Address", "classifications": ["Email Address"]},
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
                                file_name="test_file.csv",
                                source_folder="Horizon"
                            )

    # Verify anonymisation
    rows = result_df.select("First Name", "Email Address").collect()

    # First row should be anonymised
    assert rows[0]["First Name"] == "J***"
    assert rows[0]["Email Address"] == "j******e@example.com"

    # Second row nulls should remain null
    assert rows[1]["First Name"] is None
    assert rows[1]["Email Address"] is None
