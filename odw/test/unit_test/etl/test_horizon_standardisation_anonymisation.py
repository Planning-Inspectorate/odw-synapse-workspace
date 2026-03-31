from unittest import mock
import json
import pyspark.sql.functions as F
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil


def _build_horizon_file_df(spark):
    data = [
        {
            "Staff Number": "S001",
            "First Name": "John",
            "Last Name": "Doe",
            "Email Address": "john.doe@example.com",
            "Birth Date": "1990-01-01",
            "Annual Salary": "50000",
        }
    ]
    return spark.createDataFrame(data)


def _build_orchestration_df(spark):
    payload = {
        "definitions": [
            {
                "Source_Filename_Start": "test_file",
                "Load_Enable_status": "True",
                "Standardised_Table_Name": "test_table",
                "Expected_Within_Weekdays": 5,
            }
        ]
    }
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(payload)]))


def _build_schema_dict():
    return {
        "type": "struct",
        "fields": [
            {"name": "staff_number", "type": "string", "nullable": True, "metadata": {}},
            {"name": "first_name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "last_name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "email_address", "type": "string", "nullable": True, "metadata": {}},
            {"name": "birth_date", "type": "string", "nullable": True, "metadata": {}},
            {"name": "annual_salary", "type": "string", "nullable": True, "metadata": {}},
            {"name": "ingested_datetime", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "ingested_by_process_name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "expected_from", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "expected_to", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "input_file", "type": "string", "nullable": True, "metadata": {}},
            {"name": "modified_datetime", "type": "timestamp", "nullable": True, "metadata": {}},
            {"name": "modified_by_process_name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "entity_name", "type": "string", "nullable": True, "metadata": {}},
            {"name": "file_id", "type": "string", "nullable": True, "metadata": {}},
        ],
    }


def _build_anonymised_df(spark):
    data = [
        {
            "staff_number": "S001",
            "first_name": "J***",
            "last_name": "D**",
            "email_address": "j******e@example.com",
            "birth_date": "1980-06-01",
            "annual_salary": "65000",
        }
    ]
    df = spark.createDataFrame(data)
    return (
        df.withColumn("ingested_datetime", F.current_timestamp())
        .withColumn("ingested_by_process_name", F.lit("horizon_standardisation_process"))
        .withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 5 DAY"))
        .withColumn("input_file", F.lit("test_file.csv"))
        .withColumn("modified_datetime", F.current_timestamp())
        .withColumn("modified_by_process_name", F.lit("horizon_standardisation_process"))
        .withColumn("entity_name", F.lit("test_file"))
        .withColumn("file_id", F.lit("dummy-file-id"))
    )


def test__horizon_standardisation__process_applies_anonymisation_in_non_production():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_df = _build_horizon_file_df(spark)
    orchestration_df = _build_orchestration_df(spark)
    schema_dict = _build_schema_dict()
    anonymised_df = _build_anonymised_df(spark)

    process = HorizonStandardisationProcess(spark=spark)

    source_data = {
        "test_file.csv": raw_df,
        "orchestration_data": orchestration_df,
        "test_table_standardised_table_schema": schema_dict,
        "odw_standardised_db.test_table": None,
    }

    with mock.patch.object(Util, "is_non_production_environment", return_value=True), \
         mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"), \
         mock.patch.object(LoggingUtil, "__new__"), \
         mock.patch.object(LoggingUtil, "log_info", return_value=None), \
         mock.patch.object(LoggingUtil, "log_error", return_value=None), \
         mock.patch(
             "odw.core.etl.transformation.standardised.horizon_standardisation_process.AnonymisationEngine.apply_from_purview",
             return_value=anonymised_df,
         ) as mock_apply:

        data_to_write, etl_result = process.process(
            source_data=source_data.copy(),
            entity_name="test_file",
            date_folder="2025-01-05",
        )

    mock_apply.assert_called_once()
    _, kwargs = mock_apply.call_args
    assert kwargs["file_name"] == "test_file.csv"
    assert kwargs["source_folder"] == "Horizon"

    written_df = data_to_write["odw_standardised_db.test_table"]["data"]
    row = written_df.select("first_name", "last_name", "email_address").collect()[0].asDict()

    assert row["first_name"] == "J***"
    assert row["last_name"] == "D**"
    assert row["email_address"] == "j******e@example.com"
    assert etl_result.metadata.table_name == "test_table"


def test__horizon_standardisation__process_skips_anonymisation_in_production():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_df = _build_horizon_file_df(spark)
    orchestration_df = _build_orchestration_df(spark)
    schema_dict = _build_schema_dict()

    process = HorizonStandardisationProcess(spark=spark)

    source_data = {
        "test_file.csv": raw_df,
        "orchestration_data": orchestration_df,
        "test_table_standardised_table_schema": schema_dict,
        "odw_standardised_db.test_table": None,
    }

    with mock.patch.object(Util, "is_non_production_environment", return_value=False), \
         mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"), \
         mock.patch.object(LoggingUtil, "__new__"), \
         mock.patch.object(LoggingUtil, "log_info", return_value=None), \
         mock.patch.object(LoggingUtil, "log_error", return_value=None), \
         mock.patch(
             "odw.core.etl.transformation.standardised.horizon_standardisation_process.AnonymisationEngine.apply_from_purview"
         ) as mock_apply:

        data_to_write, etl_result = process.process(
            source_data=source_data.copy(),
            entity_name="test_file",
            date_folder="2025-01-05",
        )

    mock_apply.assert_not_called()

    written_df = data_to_write["odw_standardised_db.test_table"]["data"]
    row = written_df.select("first_name", "last_name", "email_address").collect()[0].asDict()

    assert row["first_name"] == "John"
    assert row["last_name"] == "Doe"
    assert row["email_address"] == "john.doe@example.com"
    assert etl_result.metadata.table_name == "test_table"


def test__horizon_standardisation__process_preserves_standard_write_contract():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_df = _build_horizon_file_df(spark)
    orchestration_df = _build_orchestration_df(spark)
    schema_dict = _build_schema_dict()
    anonymised_df = _build_anonymised_df(spark)

    process = HorizonStandardisationProcess(spark=spark)

    source_data = {
        "test_file.csv": raw_df,
        "orchestration_data": orchestration_df,
        "test_table_standardised_table_schema": schema_dict,
        "odw_standardised_db.test_table": None,
    }

    with mock.patch.object(Util, "is_non_production_environment", return_value=True), \
         mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"), \
         mock.patch.object(LoggingUtil, "__new__"), \
         mock.patch.object(LoggingUtil, "log_info", return_value=None), \
         mock.patch.object(LoggingUtil, "log_error", return_value=None), \
         mock.patch(
             "odw.core.etl.transformation.standardised.horizon_standardisation_process.AnonymisationEngine.apply_from_purview",
             return_value=anonymised_df,
         ):

        data_to_write, _ = process.process(
            source_data=source_data.copy(),
            entity_name="test_file",
            date_folder="2025-01-05",
        )

    payload = data_to_write["odw_standardised_db.test_table"]

    assert payload["storage_kind"] == "ADLSG2-Table"
    assert payload["database_name"] == "odw_standardised_db"
    assert payload["table_name"] == "test_table"
    assert payload["storage_endpoint"] == "test-storage.dfs.core.windows.net"
    assert payload["container_name"] == "odw-standardised"
    assert payload["blob_path"] == "Horizon/test_table"
    assert payload["file_format"] == "delta"
    assert payload["write_mode"] == "overwrite"
    assert payload["write_options"] == {}


def test__horizon_standardisation__process_raises_when_anonymisation_fails():
    spark = PytestSparkSessionUtil().get_spark_session()

    raw_df = _build_horizon_file_df(spark)
    orchestration_df = _build_orchestration_df(spark)
    schema_dict = _build_schema_dict()

    process = HorizonStandardisationProcess(spark=spark)

    source_data = {
        "test_file.csv": raw_df,
        "orchestration_data": orchestration_df,
        "test_table_standardised_table_schema": schema_dict,
        "odw_standardised_db.test_table": None,
    }

    with mock.patch.object(Util, "is_non_production_environment", return_value=True), \
         mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"), \
         mock.patch.object(LoggingUtil, "__new__"), \
         mock.patch.object(LoggingUtil, "log_info", return_value=None), \
         mock.patch.object(LoggingUtil, "log_error", return_value=None), \
         mock.patch(
             "odw.core.etl.transformation.standardised.horizon_standardisation_process.AnonymisationEngine.apply_from_purview",
             side_effect=RuntimeError("Purview anonymisation failed"),
         ):

        try:
            process.process(
                source_data=source_data.copy(),
                entity_name="test_file",
                date_folder="2025-01-05",
            )
            assert False, "Expected process() to raise when anonymisation fails"
        except RuntimeError as ex:
            assert str(ex) == "Purview anonymisation failed"