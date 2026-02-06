from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.util.util import Util
from odw.test.util.util import generate_local_path
from odw.test.util.session_util import PytestSparkSessionUtil
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T
import mock
from odw.test.util.assertion import assert_dataframes_equal
import json
from copy import deepcopy
import os


def test_standardise():
    """
    - Given I have a dataframe with some string, integer, and array[str] columns, with column names not being normalised
    - When i call StandardisationProcess.standardise() with the given data, and expected output schema
    - Then the output dataframe should
        - Have its columns only be alphanumeric (and lowercase), with illegal characters being replaced by "_"
        - Have the columns coerced to the types defined by the schema
        - Have columns with duplicate names being labelled by integers. i.e. "Col C, col_c -> col_c1, col_c2"
        - Have array columns converted to strings
        - Have ODW auditing columns added
    """
    spark = SparkSession.builder.getOrCreate()

    df = spark.createDataFrame(
        [
            (1, "a", 11, 11, [1, 2]),
            (2, "b", 22, 22, [3, 4]),
            (3, "c", 33, 33, [5, 6]),
            (4, "d", 44, 44, [7, 8]),
            (5, "e", 55, 55, [9])
        ],
        ["col A", "col B", "col C", "col_c", "col D"]
    )
    # The expected output schema after "standardising" the dataframe
    schema = {
        "fields": [
            {
                "metadata": {},
                "name": "col_a",
                "type": "long",
                "nullable": False
            },
            {
                "metadata": {},
                "name": "col_b",
                "type": "string",
                "nullable": False
            },
            {
                "metadata": {},
                "name": "col_c1",
                "type": "string",
                "nullable": False
            },
            {
                "metadata": {},
                "name": "col_d",
                "type": "array",
                "nullable": False
            }
        ]
    }
    expected_from = datetime.now()
    expected_to = expected_from + timedelta(days=1)
    process_name = "test_standardise"
    entity_name = "entraid"
    definition = {
        "Source_Filename_Start": entity_name
    }
    input_file = "mock_input_file"
    mock_hash = "mock_hash"
    expected_output = spark.createDataFrame(
        [
            (1, "a", "11", 11, "[1, 2]", expected_from, process_name, expected_from, expected_to, input_file, expected_from, process_name, entity_name, mock_hash),
            (2, "b", "22", 22, "[3, 4]", expected_from, process_name, expected_from, expected_to, input_file, expected_from, process_name, entity_name, mock_hash),
            (3, "c", "33", 33, "[5, 6]", expected_from, process_name, expected_from, expected_to, input_file, expected_from, process_name, entity_name, mock_hash),
            (4, "d", "44", 44, "[7, 8]", expected_from, process_name, expected_from, expected_to, input_file, expected_from, process_name, entity_name, mock_hash),
            (5, "e", "55", 55, "[9]", expected_from, process_name, expected_from, expected_to, input_file, expected_from, process_name, entity_name, mock_hash)
        ],
        T.StructType(
            [
                T.StructField("col_a", T.LongType(), True),
                T.StructField("col_b", T.StringType(), True),
                T.StructField("col_c1", T.StringType(), True),
                T.StructField("col_c2", T.LongType(), True),  # Only c1 is defined in the schema, so c2 should not be casted
                T.StructField("col_d", T.StringType(), True),
                T.StructField("ingested_datetime", T.TimestampType(), False),
                T.StructField("ingested_by_process_name", T.StringType(), False),
                T.StructField("expected_from", T.TimestampType(), False),
                T.StructField("expected_to", T.TimestampType(), False),
                T.StructField("input_file", T.StringType(), False),
                T.StructField("modified_datetime", T.TimestampType(), False),
                T.StructField("modified_by_process_name", T.StringType(), False),
                T.StructField("entity_name", T.StringType(), False),
                T.StructField("file_id", T.StringType(), False)
            ]
        )
    )
    with mock.patch.object(F, "current_timestamp", return_value=F.lit(expected_from)):
        with mock.patch.object(F, "input_file_name", return_value=F.lit(input_file)):
            with mock.patch.object(F, "sha2", return_value=F.lit(mock_hash)):
                actual_output = StandardisationProcess(spark=spark).standardise(df, schema, expected_from, expected_to, process_name, definition)
                assert_dataframes_equal(expected_output, actual_output)


def test_process__with():
    spark = SparkSession.builder.getOrCreate()

    orchestration_file = {
        "definitions": [
            {
                "Source_ID": 116,
                "Source_Folder": "test_standardisation_process",
                "Source_Frequency_Folder": "",
                "Source_Filename_Format": "test_standardisation_process.csv",
                "Source_Filename_Start": "test_standardisation_process",
                "Expected_Within_Weekdays": 1,
                "Standardised_Path": "test_standardisation_process",
                "Standardised_Table_Name": "aie_document_data",
                "Standardised_Table_Definition": "standardised_table_definitions/test_standardisation_process.json"
            }
        ]
    }

    process_arguments = {
        "source_data": {
            "test_standardisation_process1": spark.createDataFrame(
                [
                    (1, "a", 11),
                    (2, "b", 22),
                    (3, "c", 33),
                    (4, "d", 44),
                    (5, "e", 55)
                ],
                ["col A", "col B", "col C"]
            ),
            "test_standardisation_process2": spark.createDataFrame(
                [
                    (2, "f", 11),
                    (4, "g", 22),
                    (6, "h", 33),
                    (8, "i", 44),
                    (10, "j", 55)
                ],
                ["col A", "col B", "col C"]
            )
        },
        "orchestration_file": orchestration_file,
        "date_folder": "2025-01-01",
        "source_frequency_folder": "",
        "specific_file": ""
    }
    standardise_side_effects = [
        spark.createDataFrame(
            [
                (1, "a", 11),
                (2, "b", 22),
                (3, "c", 33),
                (4, "d", 44),
                (5, "e", 55)
            ],
            ["col A", "col B", "col C"]
        ),
        spark.createDataFrame(
            [
                (2, "f", 11),
                (4, "g", 22),
                (6, "h", 33),
                (8, "i", 44),
                (10, "j", 55)
            ],
            ["col A", "col B", "col C"]
        )
    ]
    expected_output = {
        "test_standardisation_process1": standardise_side_effects[0],
        "test_standardisation_process2": standardise_side_effects[1]
    }
    call_params = []
    side_effect_index = 0
    def mock_standardise(
        inst,
        data,
        schema,
        expected_from,
        expected_to,
        process_name,
        definition
    ):
        nonlocal side_effect_index
        call_params.append((data, deepcopy(schema), expected_from, expected_to, process_name, deepcopy(definition)))
        return_value =  standardise_side_effects[side_effect_index]
        side_effect_index += 1
        return return_value
    table_definitions_content = json.load(
        open(generate_local_path(os.path.join("odw-config", "standardised_table_definitions", "test_standardisation_process.json")))
    )
    expected_calls = [
        (
            standardise_side_effects[0],
            table_definitions_content,
            datetime.strptime("2025-01-01", "%Y-%m-%d") - timedelta(days=1),
            datetime.strptime("2025-01-01", "%Y-%m-%d"),
            "py_raw_to_std",
            orchestration_file["definitions"][0]
        ),
        (
            standardise_side_effects[1],
            table_definitions_content,
            datetime.strptime("2025-01-01", "%Y-%m-%d") - timedelta(days=1),
            datetime.strptime("2025-01-01", "%Y-%m-%d"),
            "py_raw_to_std",
            orchestration_file["definitions"][0]
        )
    ]

    with mock.patch.object(Util, "get_path_to_file", generate_local_path):
        with mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net/"):
            with mock.patch.object(StandardisationProcess, "standardise", mock_standardise):
                actual_output = StandardisationProcess(spark).process(
                    **process_arguments
                )
                assert expected_output == actual_output
                assert str(expected_calls) == str(call_params)
