from tests.util.pipeline_run_test_case import PipelineRunTestCase
import pytest
import json

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
                "function_name": function_name
            }
        )
        pipeline_return_value = pipeline_result["pipelineReturnValue"]
        assert "Messages" in pipeline_return_value and isinstance(pipeline_return_value["Messages"], int) and pipeline_return_value["Messages"] >= 0
