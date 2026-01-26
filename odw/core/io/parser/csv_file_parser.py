from odw.core.io.parser.data_file_parser import DataFileParser
from io import BytesIO
from pyspark.sql import DataFrame
import tempfile
import os


class CSVFileParser(DataFileParser):
    """
    Class to read/write csv files in from/to binary format
    # Example usage
    ## Reading
    ```
    spark = SparkSession.builder.getOrCreate()
    dataframe = CSVFileParser(spark).from_bytes(data_bytes)
    ```
    ## Writing

    ```
    spark = SparkSession.builder.getOrCreate()
    data_bytes = CSVFileParser(spark).to_bytes(dataframe)
    ```
    """
    @classmethod
    def get_name(cls) -> str:
        return "csv"

    def from_bytes(self, byte_stream: BytesIO) -> DataFrame:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(byte_stream.getbuffer())
            tmp_path = temp_file.name
        return self.spark.read.csv(f"file://{tmp_path}")

    def to_bytes(self, data: DataFrame) -> BytesIO:
        temp_dir = tempfile.mkdtemp()
        data.write.mode("overwrite").csv(temp_dir)
        csv_file = next(
            f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")
        )
        full_path = os.path.join(temp_dir, csv_file)
        with open(full_path, "rb") as f:
            return f.read()
