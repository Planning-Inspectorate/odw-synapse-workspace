from odw.test.util.mock.import_mock_notebook_utils import notebookutils  # noqa: F401
from odw.core.etl.transformation.standardised.horizon_standardisation_process import HorizonStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.util import generate_local_path, add_orchestration_entry
from odw.test.util.session_util import PytestSparkSessionUtil
import mock
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
import shutil
import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import pyspark.sql.types as T


class TestHorizonStandardisationProcess(ETLTestCase):
    def session_setup(self):
        super().session_setup()
        new_definitions = [
            {
                "Source_Filename_Start": "test_hzn_std_pc_exst_data",
                "Load_Enable_status": "True",
                "Standardised_Table_Definition": "standardised_table_definitions/test_hzn_std_pc_exst_data/test_hzn_std_pc_exst_data.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": "test_hzn_std_pc_exst_data",
                "Expected_Within_Weekdays": 1,
            },
            {
                "Source_Filename_Start": "test_hzn_std_pc_no_exst_data",
                "Load_Enable_status": "True",
                "Standardised_Table_Definition": "standardised_table_definitions/test_hzn_std_pc_no_exst_data/test_hzn_std_pc_no_exst_data.json",
                "Source_Frequency_Folder": "",
                "Standardised_Table_Name": "test_hzn_std_pc_no_exst_data",
                "Expected_Within_Weekdays": 1,
            },
        ]
        for definition in new_definitions:
            add_orchestration_entry(definition)

    def generate_standardised_table_definitions(self):
        return {
            "fields": [
                {"metadata": {}, "name": "col_a", "type": "string", "nullable": False},
                {"metadata": {}, "name": "col_b", "type": "string", "nullable": False},
                {"metadata": {}, "name": "col_c", "type": "string", "nullable": False},
            ]
        }

    def generate_output_table_schema(self):
        # Note: delta tables do not support non-nullable columns
        return T.StructType(
            [
                T.StructField("col_a", T.StringType(), True),
                T.StructField("col_b", T.StringType(), True),
                T.StructField("col_c", T.StringType(), True),
                T.StructField("ingested_datetime", T.TimestampType(), True),
                T.StructField("ingested_by_process_name", T.StringType(), True),
                T.StructField("expected_from", T.TimestampType(), True),
                T.StructField("expected_to", T.TimestampType(), True),
                T.StructField("input_file", T.StringType(), True),
                T.StructField("modified_datetime", T.TimestampType(), True),
                T.StructField("modified_by_process_name", T.StringType(), True),
                T.StructField("entity_name", T.StringType(), True),
                T.StructField("file_id", T.StringType(), True),
            ]
        )

    def session_teardown(self):
        super().session_teardown()
        tables_to_delete = [
            os.path.join("odw-raw", "test_hzn_std_pc_exst_data"),
            os.path.join("odw-standardised", "Horizon", "test_hzn_std_pc_exst_data"),
            os.path.join("odw-raw", "test_hzn_std_pc_no_exst_data"),
            os.path.join("odw-standardised", "Horizon", "test_hzn_std_pc_no_exst_data"),
        ]
        for table in tables_to_delete:
            shutil.rmtree(generate_local_path(table), ignore_errors=True)

    def compare_standardised_data(self, expected_df: DataFrame, actual_data: DataFrame):
        cols_to_ignore = ("ingested_datetime", "expected_from", "expected_to", "modified_datetime", "file_id")
        expected_df_cleaned = expected_df
        actual_data_cleaned = actual_data
        for col in cols_to_ignore:
            expected_df_cleaned = expected_df_cleaned.drop(col)
            actual_data_cleaned = actual_data_cleaned.drop(col)
        assert_dataframes_equal(expected_df_cleaned, actual_data_cleaned)

    def test__horizon_standardisation_process__run__with_existing_data(self):
        data_folder = "test_hzn_std_pc_exst_data"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Global mock variables
        mock_current_datetime = datetime(2027, 1, 1)
        date_folder = "2027-02-01"
        # Create mock "new" data to add to the table
        raw_csv_data = (("col_a", "col_b", "col_c"), ("a", "b", "c"), ("d", "e", "f"), ("g", "h", "i"))
        self.write_csv(raw_csv_data, ["odw-raw", data_folder, date_folder, "test_hzn_std_pc_exst_data.csv"])
        # Create mock "existing" data which is already in the table
        existing_data = spark.createDataFrame(
            (
                (
                    "j",
                    "k",
                    "l",
                    datetime(2000, 1, 1),
                    "horizon_standardisation_process",
                    datetime(2000, 1, 1),
                    datetime(2000, 1, 1),
                    "some_input_file",
                    datetime(2000, 1, 1),
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_a",
                ),
                (
                    "m",
                    "n",
                    "o",
                    datetime(2026, 1, 1),
                    "horizon_standardisation_process",
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 1),
                    "some_input_file",
                    datetime(2026, 1, 1),
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_b",
                ),
            ),
            self.generate_output_table_schema(),
        )
        self.write_existing_table(
            spark,
            existing_data,
            "test_hzn_std_pc_exst_data",
            "odw_standardised_db",
            "odw-standardised",
            "Horizon/test_hzn_std_pc_exst_data",
            "overwrite",
        )
        # Create the standardised table definitions, which outlines column casting during processing
        standardised_table_definition = self.generate_standardised_table_definitions()
        self.write_json(
            standardised_table_definition, ["odw-config", "standardised_table_definitions", data_folder, "test_hzn_std_pc_exst_data.json"]
        )

        # The expected final output after appending the new data to the existing data
        expected_table_data = spark.createDataFrame(
            (
                (
                    "j",
                    "k",
                    "l",
                    datetime(2000, 1, 1),
                    "horizon_standardisation_process",
                    datetime(2000, 1, 1),
                    datetime(2000, 1, 1),
                    "some_input_file",
                    datetime(2000, 1, 1),
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_a",
                ),
                (
                    "m",
                    "n",
                    "o",
                    datetime(2026, 1, 1),
                    "horizon_standardisation_process",
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 1),
                    "some_input_file",
                    datetime(2026, 1, 1),
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_b",
                ),
                (
                    "a",
                    "b",
                    "c",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    mock_current_datetime,
                    mock_current_datetime,
                    "some_input_file",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_c",
                ),
                (
                    "d",
                    "e",
                    "f",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    mock_current_datetime,
                    mock_current_datetime,
                    "some_input_file",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_d",
                ),
                (
                    "g",
                    "h",
                    "i",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    mock_current_datetime,
                    mock_current_datetime,
                    "some_input_file",
                    mock_current_datetime,
                    "horizon_standardisation_process",
                    "test_hzn_std_pc_exst_data",
                    "some_guid_e",
                ),
            ),
            self.generate_output_table_schema(),
        )
        # Run the full etl process
        with mock.patch.object(HorizonStandardisationProcess, "get_last_modified_folder", return_value=date_folder):
            with mock.patch.object(HorizonStandardisationProcess, "get_file_names_in_directory", return_value=["test_hzn_std_pc_exst_data.csv"]):
                with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
                    inst = HorizonStandardisationProcess(spark)
                    result = inst.run(source_folder=data_folder)
                    assert_etl_result_successful(result)
                    actual_table_data = spark.table("odw_standardised_db.test_hzn_std_pc_exst_data")
                    print("final table nullable: ", actual_table_data.schema["col_a"].nullable)
                    self.compare_standardised_data(expected_table_data, actual_table_data)

    def test__horizon_standardisation_process__run__with_no_existing_data(self):
        data_folder = "test_hzn_std_pc_no_exst_data"
        spark = PytestSparkSessionUtil().get_spark_session()
        # Global mock variables
        mock_current_datetime = datetime(2027, 1, 1)
        date_folder = "2027-02-01"
        # Create mock "new" data to add to the table
        raw_csv_data = (("col_a", "col_b", "col_c"), ("a", "b", "c"), ("d", "e", "f"), ("g", "h", "i"))
        self.write_csv(raw_csv_data, ["odw-raw", data_folder, date_folder, "test_hzn_std_pc_no_exst_data.csv"])
        # Create the standardised table definitions, which outlines column casting during processing
        standardised_table_definition = self.generate_standardised_table_definitions()
        self.write_json(
            standardised_table_definition, ["odw-config", "standardised_table_definitions", data_folder, "test_hzn_std_pc_no_exst_data.json"]
        )

        spark.sql("DROP TABLE IF EXISTS odw_standardised_db.test_hzn_std_pc_no_exst_data")

        # The expected final output after appending the new data to the existing data
        common_elements = (
            mock_current_datetime,
            "horizon_standardisation_process",
            mock_current_datetime,
            mock_current_datetime,
            "some_input_file",
            mock_current_datetime,
            "horizon_standardisation_process",
            "test_hzn_std_pc_no_exst_data",
        )
        expected_table_data = spark.createDataFrame(
            (
                ("a", "b", "c") + common_elements + ("some_guid_c",),
                ("d", "e", "f") + common_elements + ("some_guid_d",),
                ("g", "h", "i") + common_elements + ("some_guid_e",),
            ),
            self.generate_output_table_schema(),
        )
        # Run the full etl process
        with mock.patch.object(HorizonStandardisationProcess, "get_last_modified_folder", return_value=date_folder):
            with mock.patch.object(HorizonStandardisationProcess, "get_file_names_in_directory", return_value=["test_hzn_std_pc_no_exst_data.csv"]):
                with mock.patch.object(F, "input_file_name", return_value=F.lit("some_input_file")):
                    inst = HorizonStandardisationProcess(spark)
                    result = inst.run(source_folder=data_folder)
                    assert_etl_result_successful(result)
                    msg = "Expected table 'odw_standardised_db.test_hzn_std_pc_no_exst_data' to exist but it was missing"
                    assert spark.catalog.tableExists("odw_standardised_db.test_hzn_std_pc_no_exst_data"), msg
                    actual_table_data = spark.table("odw_standardised_db.test_hzn_std_pc_no_exst_data")
                    self.compare_standardised_data(expected_table_data, actual_table_data)
