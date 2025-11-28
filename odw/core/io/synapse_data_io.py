from odw.core.io.data_io import DataIO
from pyspark.sql import DataFrame


class SynapseDataIO(DataIO):
    """
    Manages data io to/from a storage location that is linked to Synapse
    """
    def read(self, **kwargs) -> DataFrame:
        """
        Read from the given storage location, and return the data as a pyspark DataFrame

        :param str storage_name: The name of the storage account to read from
        :param str container_name: The container to read from
        :param str blob_path: The path to the blob (in the container) to read
        :param SparkSession spark: The spark session
        
        :return DataFrame: The data 
        """
        pass
    
    def write(self, data: DataFrame, **kwargs):
        """
        Write the data to the given storage location
        
        :param DataFrame data: The data to write
        :param str storage_name: The name of the storage account to write to
        :param str container_name: The container to write to
        :param str blob_path: The path to the blob (in the container) to write
        :param SparkSession spark: The spark session
        """
        pass
