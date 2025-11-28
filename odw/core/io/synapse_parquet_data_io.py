from odw.core.io.synapse_data_io import SynapseDataIO
from pyspark.sql import DataFrame


class SynapseParquetDataIO(SynapseDataIO):
    @classmethod
    def get_name(cls) -> str:
        return "ADLSG2-Parquet"

    """
    Manages parquet data io to/from an Azure Data Lake Storage Gen 2 account that is linked to Synapse

    # Example usage
    ## Reading
    ```
    data_frame = ADLSDataIO().read(
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark
    )
    ```
    ## Writing

    ```
    ADLSDataIO().write(
        data_frame,
        storage_name="mystorageaccount",
        container_name="mycontainer",
        blob_path="path/to/my/file.someformat",
        spark=spark
    )
    ```
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
