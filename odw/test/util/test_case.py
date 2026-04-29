from odw.test.util.session_util import PytestSparkSessionUtil
import gc
from pyspark.sql import SparkSession, DataFrame
from odw.test.util.util import format_to_adls_path
from typing import Dict, Any
import logging
import pytest


class TestCase:
    """
    Represents a test case with setup and teardown methods. This is to support using pytest-xdist, which
    does not respect pytest's session-level fixtures

    If you need a setup or teardown to be called at the session-level with pytest-xdist, then
    create your test using the test case as the parent class, and implement one of the below methods
    """

    def session_setup(self):
        """
        Called once before testing begins
        """
        pass

    def session_teardown(self):
        """
        Called once after all tests have finished
        """
        pass

    def setup(self):
        """
        Called before each test
        """
        pass

    def teardown(self):
        """
        Called after each test
        """
        pass


class SparkTestCase(TestCase):
    """
    Represents a test case that involves spark operations
    """

    @pytest.fixture(scope="function", autouse=True)
    def teardown(self, request):
        yield
        # Clear the spark cache to free up some memory
        spark = PytestSparkSessionUtil().get_spark_session()
        spark.catalog.clearCache()
        gc.collect()

    def write_existing_table(
        self,
        spark: SparkSession,
        data: DataFrame,
        table_name: str,
        database_name: str,
        container: str,
        blob_path: str,
        mode: str,
        options: Dict[str, Any] = dict(),
    ):
        logging.info(f"Creating table '{database_name}.{table_name}'")
        spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table_name}")
        table_path = f"{database_name}.{table_name}"
        data_path = format_to_adls_path(None, container, blob_path)
        write_opts = options | {"path": data_path}
        writer = data.write.format("delta").mode(mode)
        for option, value in write_opts.items():
            writer = writer.option(option, value)
        writer.saveAsTable(table_path)
