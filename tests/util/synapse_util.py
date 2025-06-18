import struct
import pyodbc
from azure.identity import AzureCliCredential, ChainedTokenCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from tests.util.config import TEST_CONFIG
from typing import Any


SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_SERVER_VERSION = 17


class SynapseUtil:
    @classmethod
    def get_pool_connection(cls, database: str = "master"):
        """
            Get a connection to a dedicated SQL pool registered with synapse

            Example usage

            ```
            SynapseUtil.submit_sql_query(
                SynapseUtil.get_pool_connection(),
                "SELECT * FROM MY_TABLE"
            )
            ```
        """
        server = f"pins-synw-odw-{TEST_CONFIG['ENV'].lower()}-uks-ondemand.sql.azuresynapse.net"  # Convert ODBC Connection String to libpq Connection String
        return cls._get_connection(server, database)

    @classmethod
    def _get_connection(cls, server: str, database: str) -> 'pyodbc.Connection':
        """
            Get a pyodbc.Connection to a Synapse endpoint
        """
        managed_identity = ManagedIdentityCredential()
        azure_cli = AzureCliCredential()
        credential_chain = ChainedTokenCredential(managed_identity, azure_cli)
        database_token = credential_chain.get_token('https://database.windows.net/.default')
        tokenb = bytes(database_token[0], "UTF-8")
        exptoken = b''
        for i in tokenb:
            exptoken += bytes({i})
            exptoken += bytes(1)
        token_struct = struct.pack("=i", len(exptoken)) + exptoken
        conn_string = "Driver={ODBC Driver " + str(SQL_SERVER_VERSION) + " for SQL Server};SERVER=" + server + ";DATABASE=" + database + ""
        return pyodbc.connect(conn_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})

    @classmethod
    def upload_blob(cls, data: Any, target_storage_account_name: str, target_container: str, target_blob_name: str):
        blob_service_client = BlobServiceClient(f"https://{target_storage_account_name}.blob.core.windows.net", credential=AzureCliCredential())
        container_client = blob_service_client.get_container_client(container=target_container)
        container_client.upload_blob(name=target_blob_name, data=data)
    
    @classmethod
    def delete_blob(cls, target_storage_account_name: str, target_container: str, target_blob_name: str):
        blob_service_client = BlobServiceClient(f"https://{target_storage_account_name}.blob.core.windows.net", credential=AzureCliCredential())
        container_client = blob_service_client.get_container_client(container=target_container)
        container_client.delete_blob(target_blob_name)

    @classmethod
    def submit_sql_query(cls, connection: 'pyodbc.Connection', query: str) -> str:
        # Execute Query
        result = None
        try:
            cursor = connection.cursor()
            result = cursor.execute(query).fetchall()
        except pyodbc.Error as ex:
            print("SQL Error:", ex)
        finally:
            if connection:
                connection.close()
        return result
