import pytest
from odw.core.anonymisation import AnonymisationEngine, default_strategies

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pytest.skip("PySpark not installed; skipping anonymisation engine tests", allow_module_level=True)


def test_engine_applies_email_and_name_masking():
    spark = SparkSession.builder.master("local[1]").appName("anon-test").getOrCreate()
    try:
        data = [
            {"EmployeeID": "12345", "full_name": "John Doe", "email": "john.doe@example.com"},
            {"EmployeeID": "67890", "full_name": "Jane Smith", "email": "jane.smith@example.com"},
        ]
        df = spark.createDataFrame(data)

        cols = [
            {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        ]
        engine = AnonymisationEngine(strategies=default_strategies())
        out = engine.apply(df, cols)

        rows = out.select("full_name", "email").collect()
        # Name masking: first letter of first name, last letter of last name
        assert rows[0][0] == "J*** **e"
        assert rows[1][0] == "J*** ****h"
        # Email masking: mask local part and preserve domain
        assert rows[0][1] == "j******e@example.com"
        assert rows[1][1] == "j********h@example.com"
    finally:
        spark.stop()


def test_engine_logs_apply_summary_caplog(caplog):
    # Capture fallback logging from the anonymisation engine (LoggingUtil may be unavailable locally)
    caplog.set_level("INFO", logger="odw.core.anonymisation.engine")

    spark = SparkSession.builder.master("local[1]").appName("anon-test-logs").getOrCreate()
    try:
        data = [
            {"EmployeeID": "12345", "full_name": "John Doe", "email": "john.doe@example.com"},
        ]
        df = spark.createDataFrame(data)
        cols = [
            {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        ]
        engine = AnonymisationEngine(strategies=default_strategies(), run_id="test-run-123")
        _ = engine.apply(df, cols)

        msgs = "\n".join(r.message for r in caplog.records)
        assert "apply.summary" in msgs
        assert "full_name" in msgs and "email" in msgs
        assert "test-run-123" in msgs
        assert "john.doe@example.com" not in msgs
    finally:
        spark.stop()
