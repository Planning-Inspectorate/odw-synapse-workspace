from tests.util.config import TEST_CONFIG
from azure.identity import AzureCliCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import ResourceNotFoundError
from tests.util.test_case import TestCase
import requests
import pytest


class TestSmokeAzureFunctions(TestCase):
    KEY_VAULT_CLIENT = SecretClient(vault_url=f"https://pinskvsynwodw{TEST_CONFIG['ENV'].lower()}uks.vault.azure.net", credential=AzureCliCredential())

    @pytest.mark.parametrize(
        "function_name",
        [
            "folder",         
            "nsipdocument",
            "nsipexamtimetable",
            "nsipproject",
            "nsipprojectupdate",
            "nsiprepresentation",
            "s51advice",
            "nsipsubscription",
            "serviceuser",
            "appealdocument",
            "appealhas",
            "appealevent",
            "appealserviceuser",
            #"getDaRT",  # Skipping due to long-term issue with this function
            "appeals78",
            "appealrepresentation",
            #"gettimesheets",  # Skipping due to long-term issue with this function
            "appealeventestimate",
            "serviceuser"

        ]
    )
    def test_azure_function_secrets_exist(self, function_name: str):
        function_secret_name = f"function-url-{function_name.replace('-', '')}"
        try:
            self.KEY_VAULT_CLIENT.get_secret(function_secret_name).value
        except ResourceNotFoundError:
            pytest.fail("There is no secret associated with the function being tested")

    @pytest.mark.parametrize(
        "function_name,url_parameters",
        [
            ("folder", ""),
            ("nsipdocument", ""),
            ("nsipexamtimetable", ""),
            ("nsipproject", ""),
            ("nsipprojectupdate", ""),
            ("nsiprepresentation", ""),
            ("s51advice", ""),
            ("nsipsubscription", ""),
            ("serviceuser", ""),
            ("appealdocument", ""),
            ("appealhas", ""),
            ("appealevent", ""),
            ("appealserviceuser", ""),
            #("getDaRT", "&caseReference=&applicationReference="),  # Skipping due to long-term issue with this function
            ("appeals78", ""),
            ("appealrepresentation", ""),
            #("gettimesheets", "&searchCriteria="),  # Skipping due to long-term issue with this function
            ("appealeventestimate", ""),
            ("serviceuser", "")
        ]
    )
    def test_azure_function_reachable(self, function_name: str, url_parameters: str):
        function_secret_name = f"function-url-{function_name.replace('-', '')}"
        try:
            function_url = f"{self.KEY_VAULT_CLIENT.get_secret(function_secret_name).value}{url_parameters}"
        except ResourceNotFoundError:
            pytest.fail("There is no secret associated with the function being tested")
        response = requests.get(function_url)
        assert response.status_code == 200, f"Expected the status code to be 200 for function url '{function_url}', but was '{response.status_code}'"
