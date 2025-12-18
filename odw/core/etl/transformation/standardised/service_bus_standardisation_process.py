from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.util.util import Util
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
import pyspark.sql.functions as F
from pyspark.sql.types import TimestampType
from datetime import datetime
from typing import Dict
import re


class ServiceBusStandardisationProcess(StandardisationProcess):
    """
    ETL process for standardising the raw data from the Service Bus
    """
    @LoggingUtil.logging_to_appins
    def get_max_file_date(self, df: DataFrame) -> datetime:
        """
        Gets the maximum date from a file path field in a DataFrame.
        E.g. if the input_file field contained paths such as this:
        abfss://odw-raw@pinsstodwdevuks9h80mb.dfs.core.windows.net/ServiceBus/appeal-has/2024-12-02/appeal-has_2024-12-02T16:54:35.214679+0000.json
        It extracts the date from the string for each row and gets the maximum date.

        :param DataFrame df: The dataframe to analyse
        :return: The maximum file date
        """
        try:
            date_pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{4})"
            df = df.withColumn("file_date", F.regexp_extract(df["input_file"], date_pattern, 1).cast(TimestampType()))
            max_timestamp: datetime = df.agg(max("file_date")).collect()[0][0]
            return max_timestamp
        except Exception as e:
            error_message = f"Error extracting maximum file date from DataFrame: {str(e)}"
            LoggingUtil().log_error(error_message)
            raise
    
    @LoggingUtil.logging_to_appins
    def get_missing_files(self, df: DataFrame, source_path: str) -> list:
        """
        Gets the difference between the files in the source path and the files in the table.
        Converts the table column "filename" into a set.
        Creates a set containing all the files int he source path.
        Compares the two sets to give the missing files not yet loaded to the table.

        :param DataFrame df: The dataframe to analyse
        :param str source_path: The source directory to compare the dataframe against
        e.g. abfss://odw-raw@pinsstodwdevuks9h80mb.dfs.core.windows.net/ServiceBus/appeal-has/
        :return: A list of missing files not yet loaded to the table
        """
        try:
            files_in_path: set = set(self.get_all_files_in_directory(source_path))
            files_in_table: set = set(df.select("input_file").rdd.flatMap(lambda x: x).collect())
            return list(files_in_path - files_in_table)
        except Exception as e:
            error_message = f"Error getting missing files from source path '{source_path}': {str(e)}"
            LoggingUtil().log_error(error_message)
            raise
    
    @LoggingUtil.logging_to_appins
    def extract_and_filter_paths(self, files: list, filter_date: datetime):
        """
        Takes a list of file paths and filters them to return the file paths greater than the filter_date

        :param List[str] files: A list of file paths
        :param datetime filter_date: The date to filter on
        :return: A list of file paths greater than the given date
        """
        def inner_condition(file: str):
            timestamp_pattern = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}[:_]\d{2}[:_]\d{2}[.\d]*[+-]\d{4})")
            match = timestamp_pattern.search(file)
            return match and datetime.strptime(match.group(1).replace("_", ":"), "%Y-%m-%dT%H:%M:%S.%f%z") > filter_date
        return [
            file
            for file in files
            if inner_condition(file)
        ]

    @LoggingUtil.logging_to_appins
    def read_raw_messages(self, filtered_paths: list[str], spark_schema: StructType) -> DataFrame:
        """
        Ingests data from service bus messages stored as json files in the raw layer

        :param List[str] filtered_paths: The file paths to ingest
        :return: A DataFrame of service bus messages with additional columns needed for the standardised table
        """
        # Read JSON files from filtered paths
        df = self.spark.read.json(filtered_paths, schema=spark_schema)
        LoggingUtil().log_info(f"Found {df.count()} new rows.")
        # Adding the standardised columns
        return df.withColumn("expected_from", F.current_timestamp()).withColumn(
            "expected_to", F.expr("current_timestamp() + INTERVAL 1 DAY")
        ).withColumn(
            "ingested_datetime", F.to_timestamp(df.message_enqueued_time_utc)
        ).withColumn(
            "input_file", F.input_file_name()
        )
    
    @LoggingUtil.logging_to_appins
    def remove_data_duplicates(self, df: DataFrame) -> DataFrame:
        """
        Dedupes a DataFrame based on certain columns

        :param DataFrame: The dataframe to remove duplicates from
        :return: The cleaned dataframe
        """
        # removing duplicates while ignoring the ingestion dates columns
        columns_to_ignore = {"expected_to", "expected_from", "ingested_datetime"}
        df: DataFrame = df.dropDuplicates(subset=[c for c in df.columns if c not in columns_to_ignore])
        return df

    def process(self, **kwargs) -> ETLResult:
        start_exec_time = datetime.now()
        source_data: Dict[str, DataFrame] = kwargs.get("source_data", None)
        if not source_data:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a source_data dictionary to be provided, but was missing")
        if len(source_data) != 1:
            raise ValueError(f"ServiceBusStandardisationProcess.process source_data parameter must be a dictionary with 1 element, but had {len(source_data)}")
        entity_name: str = kwargs.get("entity_name", None)
        if not entity_name:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a entity_name dictionary to be provided, but was missing")
        date_folder_input: str = kwargs.get("date_folder", None)
        if not date_folder_input:
            raise ValueError(f"ServiceBusStandardisationProcess.process requires a date_folder dictionary to be provided, but was missing")
        use_max_date_filter = kwargs.get("use_max_date_filter", False)
        database_name = "odw_standardised_db"
        table_name = f"sb_{entity_name.replace('-', '_')}"
        table_path: str = f"{database_name}.{table_name}"
        source_path = Util.get_path_to_file(f"odw-raw/ServiceBus/{entity_name}")
        insert_count = 0

        table_df = list(source_data.values())[0]

        max_extracted_date = self.get_max_file_date(table_df)
        missing_files = self.get_missing_files(table_path, source_path)
        filtered_paths = []
        if use_max_date_filter:
            filtered_paths = self.extract_and_filter_paths(self.get_all_files_in_directory(source_path=source_path), max_extracted_date)
        df = self.read_raw_messages(missing_files + filtered_paths)
        table_row_count = table_df.count()
        df = self.remove_data_duplicates(df)
        insert_count = df.count()
        LoggingUtil().log_info(f"Rows to append: {insert_count}")
        expected_new_count = table_row_count + insert_count
        LoggingUtil().log_info(f"Expected new count: {expected_new_count}")
        end_exec_time = datetime.now()
        return {
            table_path: {
                "data": df,
                "database_name": database_name,
                "table_name": table_name,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-standardised",
                "blob_path": "table_name",
                "file_format": "delta",
                "write_mode": "append",
                "write_options": [
                    ("mergeSchema", "true")
                ]
            }
        }, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                error_message="",
                table_name=table_name,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds()
            )
        )
