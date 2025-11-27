from odw.core.io.parser.parquet_file_parser import ParquetFileParser
from odw.core.io.adls_data_io import ADLSDataIO
from pyspark.sql import SparkSession


data_bytes = ADLSDataIO().read(
    storage_name="pinsstodwdevuks9h80mb",
    container_name="odw-standardised",
    blob_path="Horizon/BIS_CaseSiteCategoryAdditionalStr/part-00000-094f6ef5-47cd-4c24-b9c3-208d3dc4a518-c000.snappy.parquet"
)

spark = SparkSession.builder.getOrCreate()
data = ParquetFileParser(spark).from_bytes(data_bytes)
data_bytes = ParquetFileParser(spark).to_bytes(data)

ADLSDataIO().write(
    data_bytes,
    storage_name="pinsstodwdevuks9h80mb",
    container_name="odw-standardised",
    blob_path="hbttest/someparquetfile.parquet"
)
