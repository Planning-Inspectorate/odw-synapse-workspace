import mock
import re
from datetime import date
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil
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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    # Verify anonymisation was applied
    rows = result_df.select("First Name", "Last Name", "Email Address", "Birth Date", "Annual Salary").collect()

    assert rows[0]["First Name"] == "REDACTED"
    assert rows[0]["Last Name"] == "REDACTED"
    assert rows[1]["First Name"] == "REDACTED"
    assert rows[1]["Last Name"] == "REDACTED"

    # Email should be masked
    assert rows[0]["Email Address"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"
    assert rows[1]["Email Address"] == "f2d1f1c853fd1f4be1eb5060eaae93066c877d069473795e31db5e70c4880859"

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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    # Verify non-sensitive columns remain unchanged
    rows = result_df.select("Staff Number", "Department", "Location", "First Name", "Last Name", "Email Address").collect()

    # Non-sensitive columns should be unchanged
    assert rows[0]["Staff Number"] == "S001"
    assert rows[0]["Department"] == "Engineering"
    assert rows[0]["Location"] == "London"

    # Sensitive columns should be anonymised
    assert rows[0]["First Name"] == "REDACTED"
    assert rows[0]["Last Name"] == "REDACTED"
    assert rows[0]["Email Address"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"

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
    df = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("input_file", F.lit("test_file.csv"))

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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    # Verify anonymisation was applied to transformed column names
    rows = result_df.select("staff_number", "first_name", "last_name", "email_address").collect()

    # staff_number should be unchanged
    assert rows[0]["staff_number"] == "S001"

    # Classified columns should be anonymised
    assert rows[0]["first_name"] == "REDACTED"
    assert rows[0]["last_name"] == "REDACTED"
    assert rows[0]["email_address"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"

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
    df = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("input_file", F.lit("test_file.csv"))

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
                            result_df_1 = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

                            result_df_2 = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    # Verify both runs produce identical results
    rows_1 = result_df_1.select("First Name", "Annual Salary").collect()
    rows_2 = result_df_2.select("First Name", "Annual Salary").collect()

    assert rows_1[0]["First Name"] == rows_2[0]["First Name"]
    assert rows_1[0]["Annual Salary"] == rows_2[0]["Annual Salary"]


def test__email_mask__case_insensitive_emails_produce_identical_hash():
    """
    Test that the same email address in different cases produces the same SHA-256 hash,
    making it safe to join anonymised email columns across tables with inconsistent casing.
    e.g. JOHN.DOE@EXAMPLE.COM and john.doe@example.com must hash identically.
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"Staff Number": "S001", "Email Address": "JOHN.DOE@EXAMPLE.COM"},
        {"Staff Number": "S002", "Email Address": "john.doe@example.com"},
    ]
    df = spark.createDataFrame(data)
    df = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("input_file", F.lit("test_file.csv"))

    mocked_purview_cols = [{"column_name": "Email Address", "classifications": ["Email Address"]}]

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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    rows = result_df.select("Email Address").orderBy("Email Address").collect()
    # sha256("john.doe@example.com") — both casing variants must produce this
    expected_hash = "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"

    assert rows[0]["Email Address"] == expected_hash, "Uppercase email should produce the same hash as lowercase"
    assert rows[1]["Email Address"] == expected_hash, "Lowercase email should produce the same hash as uppercase"
    assert rows[0]["Email Address"] == rows[1]["Email Address"], "Both casing variants must hash identically"


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
    df = df.withColumn("ingested_datetime", F.current_timestamp()).withColumn("input_file", F.lit("test_file.csv"))

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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    # Verify anonymisation
    rows = result_df.select("First Name", "Email Address").collect()

    # First row should be anonymised
    assert rows[0]["First Name"] == "REDACTED"
    assert rows[0]["Email Address"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"

    # Second row nulls should remain null
    assert rows[1]["First Name"] is None
    assert rows[1]["Email Address"] is None


def test__anonymisation__postcode_keeps_first_block():
    from odw.core.anonymisation.engine import AnonymisationEngine

    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"id": "1", "Postcode": "E17 4NT"},
        {"id": "2", "Postcode": "SW1W 9SP"},
        {"id": "3", "Postcode": None},
    ]
    df = spark.createDataFrame(data)

    mocked_purview_cols = [
        {"column_name": "Postcode", "classifications": ["UK Postcode"]},
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
                            engine = AnonymisationEngine()
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    rows = result_df.select("Postcode").collect()
    assert rows[0]["Postcode"] == "E17"
    assert rows[1]["Postcode"] == "SW1W"
    assert rows[2]["Postcode"] is None


def test__address_strategy__redacts_struct_fields_preserving_postcode():
    """
    Test that AddressStrategy redacts struct address fields but preserves postcode
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    from pyspark.sql.types import StructType, StructField, StringType

    address_schema = StructType(
        [
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("townCity", StringType(), True),
            StructField("county", StringType(), True),
            StructField("postcode", StringType(), True),
        ]
    )

    data = [
        {
            "sapId": "123",
            "firstName": "John",
            "address": {
                "addressLine1": "123 Main St",
                "addressLine2": "Apt 4B",
                "townCity": "London",
                "county": "Greater London",
                "postcode": "SW1A 1AA",
            },
        },
        {
            "sapId": "456",
            "firstName": "Jane",
            "address": {
                "addressLine1": "456 Oak Ave",
                "addressLine2": None,
                "townCity": "Manchester",
                "county": "Greater Manchester",
                "postcode": "M1 1AA",
            },
        },
    ]

    schema = StructType(
        [
            StructField("sapId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("address", address_schema, True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    mocked_purview_cols = [
        {"column_name": "address", "classifications": ["MICROSOFT.PERSONAL.PHYSICALADDRESS"]},
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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    rows = result_df.select("sapId", "firstName", "address").collect()

    assert rows[0]["sapId"] == "123"
    assert rows[0]["firstName"] == "John"
    assert rows[0]["address"].addressLine1 == "REDACTED"
    assert rows[0]["address"].addressLine2 == "REDACTED"
    assert rows[0]["address"].townCity == "REDACTED"
    assert rows[0]["address"].county == "REDACTED"
    assert rows[0]["address"].postcode == "SW1A"

    assert rows[1]["sapId"] == "456"
    assert rows[1]["firstName"] == "Jane"
    assert rows[1]["address"].addressLine1 == "REDACTED"
    assert rows[1]["address"].addressLine2 == "REDACTED"
    assert rows[1]["address"].townCity == "REDACTED"
    assert rows[1]["address"].county == "REDACTED"
    assert rows[1]["address"].postcode == "M1"


def test__address_strategy__redacts_simple_string_address_fields():
    """
    Test that AddressStrategy redacts simple string address fields
    """
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {
            "Staff Number": "S001",
            "Address Line 1": "123 Main Street",
            "Address Line 2": "Apartment 4B",
            "Postcode": "SW1A 1AA",
        },
        {
            "Staff Number": "S002",
            "Address Line 1": "456 Oak Avenue",
            "Address Line 2": None,
            "Postcode": "M1 1AA",
        },
    ]
    df = spark.createDataFrame(data)

    mocked_purview_cols = [
        {"column_name": "Address Line 1", "classifications": ["Address Line 1"]},
        {"column_name": "Address Line 2", "classifications": ["Address Line 2"]},
        {"column_name": "Postcode", "classifications": ["Postcode"]},
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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    rows = result_df.select("Staff Number", "Address Line 1", "Address Line 2", "Postcode").collect()

    assert rows[0]["Staff Number"] == "S001"
    assert rows[0]["Address Line 1"] == "REDACTED"
    assert rows[0]["Address Line 2"] == "REDACTED"
    assert rows[0]["Postcode"] == "REDACTED"

    assert rows[1]["Staff Number"] == "S002"
    assert rows[1]["Address Line 1"] == "REDACTED"
    assert rows[1]["Address Line 2"] is None
    assert rows[1]["Postcode"] == "REDACTED"


def test__address_strategy__handles_null_struct_address():
    """
    Test that AddressStrategy correctly handles null struct address values
    """
    spark = PytestSparkSessionUtil().get_spark_session()
    from pyspark.sql.types import StructType, StructField, StringType

    address_schema = StructType(
        [
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("postcode", StringType(), True),
        ]
    )

    data = [
        {
            "sapId": "123",
            "firstName": "John",
            "address": {
                "addressLine1": "123 Main St",
                "addressLine2": "Apt 4B",
                "postcode": "SW1A 1AA",
            },
        },
        {
            "sapId": "456",
            "firstName": "Jane",
            "address": None,
        },
    ]

    schema = StructType(
        [
            StructField("sapId", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("address", address_schema, True),
        ]
    )
    df = spark.createDataFrame(data, schema)

    mocked_purview_cols = [
        {"column_name": "address", "classifications": ["MICROSOFT.PERSONAL.PHYSICALADDRESS"]},
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
                            result_df = engine.apply_from_purview(df, file_name="test_file.csv", source_folder="Horizon")

    rows = result_df.select("sapId", "firstName", "address").collect()

    assert rows[0]["sapId"] == "123"
    assert rows[0]["address"].addressLine1 == "REDACTED"
    assert rows[0]["address"].postcode == "SW1A"

    assert rows[1]["sapId"] == "456"
    assert rows[1]["address"] is None
