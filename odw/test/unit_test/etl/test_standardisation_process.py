from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T
import mock
from odw.test.util.assertion import assert_dataframes_equal


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
