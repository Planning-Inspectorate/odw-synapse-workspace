from tests.util.pipeline_run_test_case import PipelineRunTestCase
from tests.util.config import TEST_CONFIG
import pytest


class TestIntegrationPlnTriggerFunctionApp(PipelineRunTestCase):
    @pytest.mark.parametrize(
        "function_name",
        [
            "folder",         
            # "nsipdocument",
            # "nsipexamtimetable",
            # "nsipproject",
            # "nsipprojectupdate",
            # "nsiprepresentation",
            # "s51advice",
            # "nsipsubscription",
            # "serviceuser",
            # "appealdocument",
            # "appealhas",
            # "appealevent",
            # "appealserviceuser",
            # "getDaRT",
            # "appeals78",
            # "appealrepresentation",
            # "gettimesheets",
            # "appealeventestimate",
            # "serviceuser"

        ]
    )
    def test_pln_trigger_function_app(self, function_name: str):
        pipeline_result = self.run_pipeline(
            "pln_trigger_function_app",
            {
                "function_name": function_name,
                "Key_Vault_Url": f"https://pinskvsynwodw{TEST_CONFIG['ENV'].lower()}uks.vault.azure.net/secrets/"
            },
            max_wait_time_minutes = 30
        )
        pipeline_return_value = pipeline_result["pipelineReturnValue"]
        # This pipeline always returns a Messages value, so it is not possible to check the underlying logic, only that the pipeline returns succesfully
        assert "Messages" in pipeline_return_value and isinstance(pipeline_return_value["Messages"], int) and pipeline_return_value["Messages"] >= 0
