import logging
from pyspark.sql import SparkSession
from notebookutils import mssparkutils
from odw.core.util.logging_util import LoggingUtil


logger = logging.getLogger(__name__)


class TableUtil:
    """
    Utility class for interacting with tables
    """

    @classmethod
    @LoggingUtil.logging_to_appins
    def delete_table(cls, db_name: str, table_name: str):
        """
        Delete the given table in the given database. This should be used for tables that do not use
        delta as the underlying storag mechanism

        **IMPORTANT**

        Delta recommends that if you want to only delete the content of a table, then not to
        delete the table itself as this will remove the history. Please use `delete_table_contents` if this is what
        you wish to do

        :param db_name: Name of the database the table belongs to
        :param table_name: The name of the table to delete
        """
        spark = SparkSession.builder.getOrCreate()
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            table_details_query = spark.sql(f"DESCRIBE DETAIL {db_name}.{table_name}")
            num_tables = table_details_query.count()
            if num_tables > 1:
                raise RuntimeError("too many locations associated with the table!")
            else:
                loc = table_details_query.select("location").first().location
                mssparkutils.fs.rm(loc, True)
                spark.sql(f"DROP TABLE IF EXISTS {db_name}.{table_name}")
                LoggingUtil().log_info(f"Dropped table {db_name}.{table_name}")
        else:
            LoggingUtil().log_info("Table does not exist")

    @classmethod
    @LoggingUtil.logging_to_appins
    def delete_table_contents(cls, db_name: str, table_name: str):
        """
        Delete the content from the given table in the given database. This should be used for
        tables that use delta format as the underlying storage mechanism

        :param db_name: Name of the database the table belongs to
        :param table_name: The name of the table to delete
        """
        spark = SparkSession.builder.getOrCreate()
        if spark.catalog.tableExists(f"{db_name}.{table_name}"):
            table_details_query = spark.sql(f"DESCRIBE DETAIL {db_name}.{table_name}")
            num_tables = table_details_query.count()
            if num_tables > 1:
                raise RuntimeError("too many locations associated with the table!")
            else:
                spark.sql(f"DELETE FROM {db_name}.{table_name}")
                LoggingUtil().log_info(f"Deleted the content from table {db_name}.{table_name}")
        else:
            LoggingUtil().log_info("Table does not exist")
