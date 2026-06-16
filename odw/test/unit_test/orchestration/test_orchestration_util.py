# successes = "[{\"input_parameters\":{\"kwargs\":{\"etl_process_name\":\"Fake Successful ETL Process\",\"entity_name\":\"nsip-subscription\"},\"depends_on\":[],\"orchestration_entity_stage_name\":\"nsip-subscription.service_bus_standardised\",\"orchestration_entity_name\":\"nsip-subscription\",\"orchestration_stage_name\":\"service_bus_standardised\"}}]"
from odw.core.orchestration.orchestration_util import OrchestrationUtil
from typing import Dict, Any
import pytest


def sort_key(x: Dict[str, Any]):
    return str(x.get("entity", "")) + str(x.get("stage", "")) + str(x.get("successful", ""))


def test__orchestration_util__validate():
    results = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process A"},"depends_on":[],"orchestration_entity_stage_name":"A.X","orchestration_entity_name":"A","orchestration_stage_name":"X"}},{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process B"},"depends_on":[],"orchestration_entity_stage_name":"B.X","orchestration_entity_name":"B","orchestration_stage_name":"X"}}]',
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process C"},"depends_on":[],"orchestration_entity_stage_name":"C.X","orchestration_entity_name":"C","orchestration_stage_name":"X"}}]',
    ]
    expected_results = [
        [
            {
                "input_parameters": {
                    "kwargs": {"etl_process_name": "Some ETL Process A"},
                    "orchestration_entity_stage_name": "A.X",
                    "orchestration_entity_name": "A",
                    "orchestration_stage_name": "X",
                    "depends_on": [],
                }
            },
            {
                "input_parameters": {
                    "kwargs": {"etl_process_name": "Some ETL Process B"},
                    "orchestration_entity_stage_name": "B.X",
                    "orchestration_entity_name": "B",
                    "orchestration_stage_name": "X",
                    "depends_on": [],
                }
            },
        ],
        [
            {
                "input_parameters": {
                    "kwargs": {"etl_process_name": "Some ETL Process C"},
                    "orchestration_entity_stage_name": "C.X",
                    "orchestration_entity_name": "C",
                    "orchestration_stage_name": "X",
                    "depends_on": [],
                }
            }
        ],
    ]
    actual_results = OrchestrationUtil._validate_results(results)
    assert actual_results == expected_results


def test__orchestration_util__validate__with_invalid_content():
    results = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process A"},"depends_on":[],"orchestration_entity_stage_name":"A.X","orchestration_stage_name":"X"}},{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process B"},"depends_on":[],"orchestration_entity_stage_name":"B.X","orchestration_entity_name":"B","orchestration_stage_name":"X"}}]',
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process C"},"depends_on":[],"orchestration_entity_stage_name":"C.X","orchestration_entity_name":"C","orchestration_stage_name":"X"}}]',
    ]
    # Missing orchestration_entity_name in the first entry
    with pytest.raises(ValueError) as e:
        OrchestrationUtil._validate_results(results)
        assert "orchestration_entity_name" in e.value.message


def test__orchestration_util__validate_preprocessing_failures():
    preprocessing_failures = [
        {
            "batch": [
                {
                    "kwargs": {"etl_process_name": "Some ETL Process E"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "E.X",
                    "orchestration_entity_name": "E",
                    "orchestration_stage_name": "X",
                }
            ],
            "error": "some preprocessing error",
        }
    ]
    expected_output = [
        {
            "batch": [
                {
                    "kwargs": {"etl_process_name": "Some ETL Process E"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "E.X",
                    "orchestration_entity_name": "E",
                    "orchestration_stage_name": "X",
                }
            ],
            "error": "some preprocessing error",
        }
    ]
    actual_output = OrchestrationUtil._validate_preprocessing_failures(preprocessing_failures)
    assert actual_output == expected_output


def test__orchestration_util__validate_preprocessing_failures__invalid_input__missing_error():
    preprocessing_failures = [
        {
            "batch": [
                {
                    "kwargs": {"etl_process_name": "Some ETL Process E"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "E.X",
                    "orchestration_entity_name": "E",
                    "orchestration_stage_name": "X",
                }
            ]
        }
    ]
    # Missing error in the first entry
    with pytest.raises(ValueError) as e:
        OrchestrationUtil._validate_preprocessing_failures(preprocessing_failures)
        assert "error" in e.value.message


def test__orchestration_util__validate_preprocessing_failures__invalid_input__invalid_batch():
    preprocessing_failures = [
        {
            "batch": [
                {
                    "kwargs": {"etl_process_name": "Some ETL Process E"},
                    "depends_on": [],
                    "orchestration_entity_name": "E",
                    "orchestration_stage_name": "X",
                }
            ],
            "error": "some preprocessing error",
        }
    ]
    # Missing orchestration_entity_stage_name in the first entry
    with pytest.raises(ValueError) as e:
        OrchestrationUtil._validate_preprocessing_failures(preprocessing_failures)
        assert "orchestration_entity_stage_name" in e.value.message


def test__orchestration_util__postprocess_orchestration_results__with_failures():
    successes = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process A"},"depends_on":[],"orchestration_entity_stage_name":"A.X","orchestration_entity_name":"A","orchestration_stage_name":"X"}},{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process B"},"depends_on":[],"orchestration_entity_stage_name":"B.X","orchestration_entity_name":"B","orchestration_stage_name":"X"}}]',
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process C"},"depends_on":[],"orchestration_entity_stage_name":"C.X","orchestration_entity_name":"C","orchestration_stage_name":"X"}}]',
    ]
    failures = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process D"},"depends_on":[],"orchestration_entity_stage_name":"D.X","orchestration_entity_name":"D","orchestration_stage_name":"X"},"error":"Some error message"}]'
    ]
    expected_results = sorted(
        [
            {"entity": "A", "stage": "X", "successful": True, "error": None},
            {"entity": "B", "stage": "X", "successful": True, "error": None},
            {"entity": "C", "stage": "X", "successful": True, "error": None},
            {"entity": "D", "stage": "X", "successful": False, "error": "Some error message"},
        ],
        key=sort_key,
    )
    actual_result = OrchestrationUtil.postprocess_orchestration_results(successes, failures, [])
    assert actual_result.get("hasFailure", None)
    actual_result_sorted = sorted(actual_result.get("results", []), key=sort_key)
    assert actual_result_sorted == expected_results


def test__orchestration_util__postprocess_orchestration_results__with_no_failures():
    successes = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process A"},"depends_on":[],"orchestration_entity_stage_name":"A.X","orchestration_entity_name":"A","orchestration_stage_name":"X"}},{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process B"},"depends_on":[],"orchestration_entity_stage_name":"B.X","orchestration_entity_name":"B","orchestration_stage_name":"X"}}]',
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process C"},"depends_on":[],"orchestration_entity_stage_name":"C.X","orchestration_entity_name":"C","orchestration_stage_name":"X"}}]',
    ]
    failures = []
    expected_results = sorted(
        [
            {"entity": "A", "stage": "X", "successful": True, "error": None},
            {"entity": "B", "stage": "X", "successful": True, "error": None},
            {"entity": "C", "stage": "X", "successful": True, "error": None},
        ],
        key=sort_key,
    )
    actual_result = OrchestrationUtil.postprocess_orchestration_results(successes, failures, [])
    assert actual_result.get("hasFailure", None) is False
    actual_result_sorted = sorted(actual_result.get("results", []), key=sort_key)
    assert actual_result_sorted == expected_results


def test__orchestration_util__postprocess_orchestration_results__with_preprocessing_failures():
    successes = [
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process A"},"depends_on":[],"orchestration_entity_stage_name":"A.X","orchestration_entity_name":"A","orchestration_stage_name":"X"}},{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process B"},"depends_on":[],"orchestration_entity_stage_name":"B.X","orchestration_entity_name":"B","orchestration_stage_name":"X"}}]',
        '[{"input_parameters":{"kwargs":{"etl_process_name":"Some ETL Process C"},"depends_on":[],"orchestration_entity_stage_name":"C.X","orchestration_entity_name":"C","orchestration_stage_name":"X"}}]',
    ]
    failures = []
    preprocessing_failures = [
        {
            "batch": [
                {
                    "kwargs": {"etl_process_name": "Some ETL Process E"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "E.X",
                    "orchestration_entity_name": "E",
                    "orchestration_stage_name": "X",
                }
            ],
            "error": "some preprocessing error",
        }
    ]
    expected_results = sorted(
        [
            {"entity": "A", "stage": "X", "successful": True, "error": None},
            {"entity": "B", "stage": "X", "successful": True, "error": None},
            {"entity": "C", "stage": "X", "successful": True, "error": None},
            {"entity": "E", "stage": "X", "successful": False, "error": "some preprocessing error"},
        ],
        key=sort_key,
    )
    actual_result = OrchestrationUtil.postprocess_orchestration_results(successes, failures, preprocessing_failures)
    assert actual_result.get("hasFailure", None)
    actual_result_sorted = sorted(actual_result.get("results", []), key=sort_key)
    assert actual_result_sorted == expected_results
