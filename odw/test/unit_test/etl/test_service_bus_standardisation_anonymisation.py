from unittest import mock
import pyspark.sql.functions as F
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.test.util.session_util import PytestSparkSessionUtil


def _build_existing_table_df(spark):
    data = [
        {
            "EmployeeID": "EXISTING-1",
            "full_name": "Existing Person",
            "emailAddress": "existing@example.com",
            "message_enqueued_time_utc": "2025-01-01T00:00:00.000000+0000",
            "expected_from": "2025-01-01T00:00:00",
            "expected_to": "2025-01-02T00:00:00",
            "ingested_datetime": "2025-01-01T00:00:00",
            "input_file": "existing.json",
        }
    ]
    df = spark.createDataFrame(data)
    return (
        df.withColumn("expected_from", F.to_timestamp("expected_from"))
        .withColumn("expected_to", F.to_timestamp("expected_to"))
        .withColumn("ingested_datetime", F.to_timestamp("ingested_datetime"))
    )


def _build_raw_messages_df(spark):
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "John Doe",
            "emailAddress": "john.doe@example.com",
            "message_enqueued_time_utc": "2025-01-02T00:00:00.000000+0000",
        },
        {
            "EmployeeID": "E2",
            "full_name": "Jane Smith",
            "emailAddress": "jane.smith@example.com",
            "message_enqueued_time_utc": "2025-01-03T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    return (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )


def _build_anonymised_df(spark):
    data = [
        {
            "EmployeeID": "E1",
            "full_name": "J*** **e",
            "emailAddress": "j******e@example.com",
            "message_enqueued_time_utc": "2025-01-02T00:00:00.000000+0000",
        },
        {
            "EmployeeID": "E2",
            "full_name": "J*** ****h",
            "emailAddress": "j********h@example.com",
            "message_enqueued_time_utc": "2025-01-03T00:00:00.000000+0000",
        },
    ]
    df = spark.createDataFrame(data)
    return (
        df.withColumn("expected_from", F.current_timestamp())
        .withColumn("expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY"))
        .withColumn("ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc))
        .withColumn("input_file", F.lit("test_file.json"))
    )


def test__service_bus_standardisation__process_applies_anonymisation_in_non_production():
    spark = PytestSparkSessionUtil().get_spark_session()

    table_df = _build_existing_table_df(spark)
    raw_df = _build_raw_messages_df(spark)
    anonymised_df = _build_anonymised_df(spark)

    process = ServiceBusStandardisationProcess(spark=spark)

    source_data = {
        "odw_standardised_db.sb_service_user": table_df,
        "raw_messages": raw_df,
    }

    with (
        mock.patch.object(Util, "is_non_production_environment", return_value=True),
        mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"),
        mock.patch.object(LoggingUtil, "__new__"),
        mock.patch.object(LoggingUtil, "log_info", return_value=None),
        mock.patch.object(LoggingUtil, "log_error", return_value=None),
        mock.patch(
            "odw.core.etl.transformation.standardised.service_bus_standardisation_process.AnonymisationEngine.apply_from_purview",
            return_value=anonymised_df,
        ) as mock_apply,
    ):
        data_to_write, etl_result = process.process(
            source_data=source_data.copy(),
            entity_name="service-user",
        )

    mock_apply.assert_called_once()
    _, kwargs = mock_apply.call_args
    assert kwargs["entity_name"] == "service-user"
    assert kwargs["source_folder"] == "ServiceBus"

    written_df = data_to_write["odw_standardised_db.sb_service_user"]["data"]
    rows = {
        (row["full_name"], row["emailAddress"])
        for row in written_df.select("full_name", "emailAddress").collect()
    }

    assert rows == {
        ("J*** **e", "j******e@example.com"),
        ("J*** ****h", "j********h@example.com"),
    }
    assert etl_result.metadata.table_name == "sb_service_user"


def test__service_bus_standardisation__process_skips_anonymisation_in_production():
    spark = PytestSparkSessionUtil().get_spark_session()

    table_df = _build_existing_table_df(spark)
    raw_df = _build_raw_messages_df(spark)

    process = ServiceBusStandardisationProcess(spark=spark)

    source_data = {
        "odw_standardised_db.sb_service_user": table_df,
        "raw_messages": raw_df,
    }

    with (
        mock.patch.object(Util, "is_non_production_environment", return_value=False),
        mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"),
        mock.patch.object(LoggingUtil, "__new__"),
        mock.patch.object(LoggingUtil, "log_info", return_value=None),
        mock.patch.object(LoggingUtil, "log_error", return_value=None),
        mock.patch(
            "odw.core.etl.transformation.standardised.service_bus_standardisation_process.AnonymisationEngine.apply_from_purview"
        ) as mock_apply,
    ):
        data_to_write, etl_result = process.process(
            source_data=source_data.copy(),
            entity_name="service-user",
        )

    mock_apply.assert_not_called()

    written_df = data_to_write["odw_standardised_db.sb_service_user"]["data"]
    rows = {
        (row["full_name"], row["emailAddress"])
        for row in written_df.select("full_name", "emailAddress").collect()
    }

    assert rows == {
        ("John Doe", "john.doe@example.com"),
        ("Jane Smith", "jane.smith@example.com"),
    }
    assert etl_result.metadata.table_name == "sb_service_user"


def test__service_bus_standardisation__process_preserves_standard_write_contract():
    spark = PytestSparkSessionUtil().get_spark_session()

    table_df = _build_existing_table_df(spark)
    raw_df = _build_raw_messages_df(spark)
    anonymised_df = _build_anonymised_df(spark)

    process = ServiceBusStandardisationProcess(spark=spark)

    source_data = {
        "odw_standardised_db.sb_service_user": table_df,
        "raw_messages": raw_df,
    }

    with (
        mock.patch.object(Util, "is_non_production_environment", return_value=True),
        mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"),
        mock.patch.object(LoggingUtil, "__new__"),
        mock.patch.object(LoggingUtil, "log_info", return_value=None),
        mock.patch.object(LoggingUtil, "log_error", return_value=None),
        mock.patch(
            "odw.core.etl.transformation.standardised.service_bus_standardisation_process.AnonymisationEngine.apply_from_purview",
            return_value=anonymised_df,
        ),
    ):
        data_to_write, _ = process.process(
            source_data=source_data.copy(),
            entity_name="service-user",
        )

    payload = data_to_write["odw_standardised_db.sb_service_user"]

    assert payload["storage_kind"] == "ADLSG2-Table"
    assert payload["database_name"] == "odw_standardised_db"
    assert payload["table_name"] == "sb_service_user"
    assert payload["storage_endpoint"] == "test-storage.dfs.core.windows.net"
    assert payload["container_name"] == "odw-standardised"
    assert payload["blob_path"] == "sb_service_user"
    assert payload["file_format"] == "delta"
    assert payload["write_mode"] == "append"
    assert payload["write_options"] == {"mergeSchema": "true"}


def test__service_bus_standardisation__process_raises_when_anonymisation_fails():
    spark = PytestSparkSessionUtil().get_spark_session()

    table_df = _build_existing_table_df(spark)
    raw_df = _build_raw_messages_df(spark)

    process = ServiceBusStandardisationProcess(spark=spark)

    source_data = {
        "odw_standardised_db.sb_service_user": table_df,
        "raw_messages": raw_df,
    }

    with (
        mock.patch.object(Util, "is_non_production_environment", return_value=True),
        mock.patch.object(Util, "get_storage_account", return_value="test-storage.dfs.core.windows.net"),
        mock.patch.object(LoggingUtil, "__new__"),
        mock.patch.object(LoggingUtil, "log_info", return_value=None),
        mock.patch.object(LoggingUtil, "log_error", return_value=None),
        mock.patch(
            "odw.core.etl.transformation.standardised.service_bus_standardisation_process.AnonymisationEngine.apply_from_purview",
            side_effect=RuntimeError("Purview anonymisation failed"),
        ),
    ):
        try:
            process.process(
                source_data=source_data.copy(),
                entity_name="service-user",
            )
            assert False, "Expected process() to raise when anonymisation fails"
        except RuntimeError as ex:
            assert str(ex) == "Purview anonymisation failed"
