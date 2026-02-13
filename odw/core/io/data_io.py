from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class DataIO(ABC):
    """
    Abstract class to manage IO for data to/from storage
    """

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        pass

    @abstractmethod
    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame

        :return DataFrame: The data
        """

    @abstractmethod
    def write(self, data: DataFrame, **kwargs):
        """
        Write the given data to the given storage location

        :param DataFrame data: The data to write
        """
