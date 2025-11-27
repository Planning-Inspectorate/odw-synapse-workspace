from abc import ABC, abstractmethod
from io import BytesIO


class DataIO(ABC):
    """
    Abstract class to manage IO for data to/from storage
    """
    @abstractmethod
    def read(self, **kwargs) -> BytesIO:
        """
        Read from the given storage location, and return the data as a byte stream
        
        :return BytesIO: A data as a byte stream
        """
    
    @abstractmethod
    def write(self, data_bytes: BytesIO, **kwargs):
        """
        Write the data bytes stream to the given storage location
        
        :param BytesIO data_bytes: The data to write
        """
