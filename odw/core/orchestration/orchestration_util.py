from odw.core.orchestration.orchestration_config import PreprocessedOrchestrationEntry
from typing import List, Dict, Any, Optional
from pydantic import BaseModel, ValidationError
import json


ResultType = List[str]


class OrchestrationResult(BaseModel):
    input_parameters: PreprocessedOrchestrationEntry
    errors: Optional[str] = None


class OrchestrationUtil:
    @classmethod
    def _validate_results(cls, results: ResultType):
        if not isinstance(results, list):
            raise ValueError(f"results should be a list, but was of type {type(results)}")
        results_json = [json.loads(x) for x in results]
        invalid_entries = []
        for result_list in results_json:
            for result in result_list:
                print(f"validating: {result}")
                try:
                    OrchestrationResult.model_validate(result)
                except ValidationError as e:
                    invalid_entries.append((result, e))

        if invalid_entries:
            raise ValueError(f"The following results are not valid: {json.dumps(invalid_entries, indent=4, default=str)}")
        return results_json

    @classmethod
    def _validate_batch_failures(cls, batch_failures: List[Dict[str, Any]]):
        pass

    @classmethod
    def _clean_results(cls, results: List[List[Dict[str, Any]]]):
        return [
            {
                "entity": result["input_parameters"]["orchestration_entity_name"],
                "stage": result["input_parameters"]["orchestration_stage_name"],
                "successful": "error" not in result,
                "error": result.get("error", None),
            }
            for batch in results
            for result in batch
        ]

    @classmethod
    def postprocess_orchestration_results(cls, successful_results: ResultType, failed_results: ResultType, batch_failures: List[Dict[str, Any]]):
        successful_results_json = cls._validate_results(failed_results)
        failed_results_json = cls._validate_results(successful_results)
        batch_failures_json = cls._validate_batch_failures(batch_failures)

        clean_successful_results = cls._clean_results(successful_results_json)
        clean_failed_results = cls._clean_results(failed_results_json)
        results = clean_successful_results + clean_failed_results
        return {"hasFailure": bool(failed_results), "results": results}
