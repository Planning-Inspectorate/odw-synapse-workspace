from odw.core.io.parser.data_file_parser import DataFileParser
from io import BytesIO
from pyspark.sql import DataFrame
import tempfile
import os


class JsonFileParser(DataFileParser):
    """
    Class to read/write json files in from/to binary format
    # Example usage
    ## Reading
    ```
    spark = SparkSession.builder.getOrCreate()
    dataframe = JsonFileParser(spark).from_bytes(data_bytes)
    ```
    ## Writing

    ```
    spark = SparkSession.builder.getOrCreate()
    data_bytes = JsonFileParser(spark).to_bytes(dataframe)
    ```
    """
    @classmethod
    def get_name(cls) -> str:
        return "json"

    def from_bytes(self, byte_stream: BytesIO) -> DataFrame:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            temp_file.write(byte_stream.getbuffer())
            tmp_path = temp_file.name
        return self.spark.read.option("multiline", "true").json(f"file://{tmp_path}")

    def to_bytes(self, data: DataFrame) -> BytesIO:
        temp_dir = tempfile.mkdtemp()
        data.write.mode("overwrite").json(temp_dir)
        json_file = next(
            f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".json")
        )
        full_path = os.path.join(temp_dir, json_file)
        with open(full_path, "rb") as f:
            return f.read()
