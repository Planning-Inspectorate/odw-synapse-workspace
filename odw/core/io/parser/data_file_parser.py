from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from io import BytesIO


class DataFileParser(ABC):
    """
    Abstract class that outlines functionality to read/write dataframe from/to binary format
    """
    def __init__(self, spark: SparkSession):
        self.spark = spark

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        pass

    @abstractmethod
    def from_bytes(self, byte_stream: BytesIO) -> DataFrame:
        """
        Read the given byte stream as a pyspark dataframe

        :param BytesIO byte_stream: The bytes to read
        :return DataFrame: The byte stream as a pyspark DataFrame
        """
        pass

    @abstractmethod
    def to_bytes(self, data: DataFrame) -> BytesIO:
        """
        Write the given pyspark dataframe to a byte stream

        :param DataFrame data: The data to convert to binary
        :return BytesIO: The data in binary format
        """
        pass
