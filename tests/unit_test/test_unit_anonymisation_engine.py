import pytest

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed; skipping anonymisation engine tests")
from pyspark.sql import SparkSession, functions as F

from odw.core.anonymisation import AnonymisationEngine, default_strategies


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
