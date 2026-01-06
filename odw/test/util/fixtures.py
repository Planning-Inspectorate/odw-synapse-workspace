from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_sesson() -> SparkSession:
    return SparkSession.builder.getOrCreate()
