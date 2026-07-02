from odw.core.orchestration.dependency_resolver import DependencyResolver
from odw.core.orchestration.orchestration_config import OrchestrationConfig
from pydantic import ValidationError
from graphlib import CycleError
import pytest
import mock


def test__dependency_resolver__init():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    expected_preprocessed_config = example_config = {
        "entities": {
            "entity-a": {
                "standardised": {
                    "etl_process": "A-S",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "entity-a.standardised",
                    "orchestration_entity_name": "entity-a",
                    "orchestration_stage_name": "standardised",
                },
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.standardised"],
                    "orchestration_entity_stage_name": "entity-a.harmonised",
                    "orchestration_entity_name": "entity-a",
                    "orchestration_stage_name": "harmonised",
                },
            },
            "entity-b": {
                "standardised": {
                    "etl_process": "B-S",
                    "kwargs": {"entity_name": "B"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "entity-b.standardised",
                    "orchestration_entity_name": "entity-b",
                    "orchestration_stage_name": "standardised",
                },
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.harmonised"],
                    "orchestration_entity_stage_name": "entity-b.curated",
                    "orchestration_entity_name": "entity-b",
                    "orchestration_stage_name": "curated",
                },
            },
            "entity-c": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-b.curated"],
                    "orchestration_entity_stage_name": "entity-c.curated",
                    "orchestration_entity_name": "entity-c",
                    "orchestration_stage_name": "curated",
                }
            },
        }
    }
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        dr = DependencyResolver(example_config)
        assert dr.config == expected_preprocessed_config


def test__dependency_resolver__topological_sort():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    expected_output = [
        [
            {
                "orchestration_entity_stage_name": "entity-a.standardised",
                "orchestration_entity_name": "entity-a",
                "orchestration_stage_name": "standardised",
                "etl_process": "A-S",
                "kwargs": {"entity_name": "A"},
                "depends_on": [],
            },
            {
                "orchestration_entity_stage_name": "entity-b.standardised",
                "orchestration_entity_name": "entity-b",
                "orchestration_stage_name": "standardised",
                "etl_process": "B-S",
                "kwargs": {"entity_name": "B"},
                "depends_on": [],
            },
        ],
        [
            {
                "orchestration_entity_stage_name": "entity-a.harmonised",
                "orchestration_entity_name": "entity-a",
                "orchestration_stage_name": "harmonised",
                "etl_process": "A-H",
                "kwargs": {"entity_name": "A"},
                "depends_on": ["entity-a.standardised"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "entity-b.curated",
                "orchestration_entity_name": "entity-b",
                "orchestration_stage_name": "curated",
                "etl_process": "A",
                "kwargs": {"entity_name": "A"},
                "depends_on": ["entity-a.harmonised"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "entity-c.curated",
                "orchestration_entity_name": "entity-c",
                "orchestration_stage_name": "curated",
                "etl_process": "A",
                "kwargs": {"entity_name": "A"},
                "depends_on": ["entity-b.curated"],
            }
        ],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config)._topological_sort(example_config)
        assert actual_output == expected_output


def test__dependency_resolver__topological_sort__complex_example():
    example_config = {
        "entities": {
            "A": {"a": {"name": "A", "depends_on": ["B.a", "C.a"]}},
            "B": {"a": {"name": "B", "depends_on": ["C.a"]}},
            "C": {"a": {"name": "C", "depends_on": ["D.a"]}},
            "D": {"a": {"name": "D", "depends_on": []}},
            "E": {"a": {"name": "E", "depends_on": ["D.a"]}},
            "F": {"a": {"name": "F", "depends_on": ["B.a", "D.a"]}},
        }
    }
    expected_output = [
        [
            {
                "orchestration_entity_stage_name": "D.a",
                "orchestration_entity_name": "D",
                "orchestration_stage_name": "a",
                "name": "D",
                "depends_on": [],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {
                "orchestration_entity_stage_name": "E.a",
                "orchestration_entity_name": "E",
                "orchestration_stage_name": "a",
                "name": "E",
                "depends_on": ["D.a"],
            },
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "A.a",
                "orchestration_entity_name": "A",
                "orchestration_stage_name": "a",
                "name": "A",
                "depends_on": ["B.a", "C.a"],
            },
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            },
        ],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config)._topological_sort(example_config)
        assert actual_output == expected_output


def test__dependency_resolver__topological_sort__with_cycle():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.standardised", "entity-c.curated"],
                },  # cycle
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        with pytest.raises(CycleError):
            DependencyResolver(example_config)._topological_sort(example_config)


def test__dependency_resolver__filter_irrelevant_dependencies_from_config():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},  # Should be filtered out
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    required_entity_stages = ["entity-c.curated"]
    expected_output = {
        "entities": {
            "entity-a": {
                "standardised": {
                    "etl_process": "A-S",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": [],
                    "orchestration_entity_stage_name": "entity-a.standardised",
                    "orchestration_entity_name": "entity-a",
                    "orchestration_stage_name": "standardised",
                },
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.standardised"],
                    "orchestration_entity_stage_name": "entity-a.harmonised",
                    "orchestration_entity_name": "entity-a",
                    "orchestration_stage_name": "harmonised",
                },
            },
            "entity-b": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.harmonised"],
                    "orchestration_entity_stage_name": "entity-b.curated",
                    "orchestration_entity_name": "entity-b",
                    "orchestration_stage_name": "curated",
                },
            },
            "entity-c": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-b.curated"],
                    "orchestration_entity_stage_name": "entity-c.curated",
                    "orchestration_entity_name": "entity-c",
                    "orchestration_stage_name": "curated",
                }
            },
        }
    }
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        dr = DependencyResolver(example_config)
        dr._filter_irrelevant_dependencies_from_config(required_entity_stages)
        assert dr.config == expected_output


def test__dependency_resolver__filter_already_executed_entity_stages():
    topological_order = [
        [
            {
                "orchestration_entity_stage_name": "D.a",
                "orchestration_entity_name": "D",
                "orchestration_stage_name": "a",
                "name": "D",
                "depends_on": [],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {"orchestration_entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]},
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "A.a",
                "orchestration_entity_name": "A",
                "orchestration_stage_name": "a",
                "name": "A",
                "depends_on": ["B.a", "C.a"],
            },
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            },
        ],
    ]
    execution_details = [
        {
            "run_id": "t_dr_faees",
            "entity_name": "D",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "C",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": False,
            "result_text": "some error message",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "E",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "B",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "A",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "F",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
        },
    ]
    expected_output = [
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {"orchestration_entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]},
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            }
        ],
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        actual_output = DependencyResolver(None).filter_already_executed_entity_stages(topological_order, execution_details)
        assert actual_output == expected_output


def test__dependency_resolver__filter_already_executed_entity_stages__with_missing_entries():
    topological_order = [
        [
            {
                "orchestration_entity_stage_name": "D.a",
                "orchestration_entity_name": "D",
                "orchestration_stage_name": "a",
                "name": "D",
                "depends_on": [],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {"orchestration_entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]},
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "A.a",
                "orchestration_entity_name": "A",
                "orchestration_stage_name": "a",
                "name": "A",
                "depends_on": ["B.a", "C.a"],
            },
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            },
        ],
    ]
    # B.a is missing, so should not be filtered out
    execution_details = [
        {
            "run_id": "t_dr_faees",
            "entity_name": "D",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "C",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": False,
            "result_text": "some error message",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "E",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "A",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_faees",
            "entity_name": "F",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": None,
            "successful": None,
            "result_text": None,
        },
    ]
    expected_output = [
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {"orchestration_entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]},
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            }
        ],
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        actual_output = DependencyResolver(None).filter_already_executed_entity_stages(topological_order, execution_details)
        assert actual_output == expected_output


def test__dependency_resolver__filter_already_executed_entity_stages__with_empty_execution_details():
    topological_order = [
        [
            {
                "orchestration_entity_stage_name": "D.a",
                "orchestration_entity_name": "D",
                "orchestration_stage_name": "a",
                "name": "D",
                "depends_on": [],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "C.a",
                "orchestration_entity_name": "C",
                "orchestration_stage_name": "a",
                "name": "C",
                "depends_on": ["D.a"],
            },
            {"orchestration_entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]},
        ],
        [
            {
                "orchestration_entity_stage_name": "B.a",
                "orchestration_entity_name": "B",
                "orchestration_stage_name": "a",
                "name": "B",
                "depends_on": ["C.a"],
            }
        ],
        [
            {
                "orchestration_entity_stage_name": "A.a",
                "orchestration_entity_name": "A",
                "orchestration_stage_name": "a",
                "name": "A",
                "depends_on": ["B.a", "C.a"],
            },
            {
                "orchestration_entity_stage_name": "F.a",
                "orchestration_entity_name": "F",
                "orchestration_stage_name": "a",
                "name": "F",
                "depends_on": ["B.a", "D.a"],
            },
        ],
    ]
    execution_details = []
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        actual_output = DependencyResolver(None).filter_already_executed_entity_stages(topological_order, execution_details)
        assert actual_output == topological_order


def test__dependency_resolver__generate_stages_to_run():
    with (
        mock.patch.object(DependencyResolver, "__init__", return_value=None),
        mock.patch.object(DependencyResolver, "_filter_irrelevant_dependencies_from_config", return_value="A"),
        mock.patch.object(DependencyResolver, "_topological_sort", return_value="B"),
        mock.patch.object(DependencyResolver, "filter_already_executed_entity_stages", return_value="C"),
        mock.patch.object(DependencyResolver, "_preprocess_entity_stages", return_value="D"),
    ):
        dr = DependencyResolver(None)
        config = "X"
        dr.config = config
        entity_stages = "Y"
        execution_details = "Z"
        result = dr.generate_stages_to_run(entity_stages, execution_details)
        DependencyResolver._preprocess_entity_stages.assert_called_once_with(entity_stages)
        DependencyResolver._filter_irrelevant_dependencies_from_config.assert_called_once_with("D")
        DependencyResolver._topological_sort.assert_called_once_with("X")
        DependencyResolver.filter_already_executed_entity_stages.assert_called_once_with("B", execution_details)
        assert result == DependencyResolver.filter_already_executed_entity_stages.return_value


def test__dependency_resolver__filter_entity_stages_with_failed_dependencies():
    group = [
        {
            "orchestration_entity_stage_name": "A.a",
            "orchestration_entity_name": "A",
            "orchestration_stage_name": "a",
            "name": "A",
            "depends_on": ["E.a"],  # Should be filtered out
        },
        {
            "orchestration_entity_stage_name": "B.a",
            "orchestration_entity_name": "B",
            "orchestration_stage_name": "a",
            "name": "B",
            "depends_on": ["F.a"],
        },
        {
            "orchestration_entity_stage_name": "C.a",
            "orchestration_entity_name": "C",
            "orchestration_stage_name": "a",
            "name": "C",
            "depends_on": ["G.a", "E.a"],  # Should be filtered out
        },
        {
            "orchestration_entity_stage_name": "D.a",
            "orchestration_entity_name": "D",
            "orchestration_stage_name": "a",
            "name": "D",
            "depends_on": ["H.a"],
        },
    ]
    execution_details = [
        {
            "run_id": "t_dr_feswfd",
            "entity_name": "E",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": False,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_feswfd",
            "entity_name": "F",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_feswfd",
            "entity_name": "G",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
        {
            "run_id": "t_dr_feswfd",
            "entity_name": "H",
            "stage_name": "a",
            "execution_parameters": '{"some": "parameters"}',
            "execution_start_time": "2026-01-01 00:00:00.000000",
            "execution_finish_time": "2026-01-01 01:00:00.000000",
            "successful": True,
            "result_text": "some text",
        },
    ]
    expected_output = [
        {
            "orchestration_entity_stage_name": "B.a",
            "orchestration_entity_name": "B",
            "orchestration_stage_name": "a",
            "name": "B",
            "depends_on": ["F.a"],
        },
        {
            "orchestration_entity_stage_name": "D.a",
            "orchestration_entity_name": "D",
            "orchestration_stage_name": "a",
            "name": "D",
            "depends_on": ["H.a"],
        },
    ]
    actual_output = DependencyResolver.filter_entity_stages_with_failed_dependencies(group, execution_details)
    assert actual_output == expected_output


def test__dependency_resolver__preprocess_entity_stages__valid():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = ["entity-a.standardised", "entity-a.harmonised", "entity-b.curated", "entity-c.curated"]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        inst = DependencyResolver()
        inst.config = config
        actual_output = inst._preprocess_entity_stages(entity_stages)
        assert set(actual_output) == set(entity_stages)


def test__dependency_resolver__preprocess_entity_stages__with_duplicates():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = [
        "entity-a.standardised",
        "entity-a.harmonised",
        "entity-a.harmonised",  # This is a duplicate
        "entity-b.curated",
        "entity-c.curated",
        "entity-c.curated",  # This is a duplicate
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        inst = DependencyResolver()
        inst.config = config
        actual_output = inst._preprocess_entity_stages(entity_stages)
        assert set(actual_output) == set(entity_stages)


def test__dependency_resolver__preprocess_entity_stages__entry_with_more_than_one_delimiter_character():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = ["entity-a.standardised", "entity-a.harmonised.somethingbad", "entity-b.curated", "entity-c.curated"]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        with pytest.raises(ValueError) as e:
            inst = DependencyResolver()
            inst.config = config
            inst._preprocess_entity_stages(entity_stages)
            assert "entity-a.harmonised.somethingbad" in e.value.message


def test__dependency_resolver__preprocess_entity_stages__exploded_entry():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = [
        "entity-a.standardised",
        "entity-a.harmonised",
        "entity-b",  # This entry should be exploded
        "entity-c.curated",
    ]
    expected_entity_stages = [
        "entity-a.standardised",
        "entity-a.harmonised",
        "entity-b.standardised",  # Should be exploded
        "entity-b.curated",  # Should be exploded
        "entity-c.curated",
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        inst = DependencyResolver()
        inst.config = config
        actual_output = inst._preprocess_entity_stages(entity_stages)
        assert set(actual_output) == set(expected_entity_stages)


def test__dependency_resolver__preprocess_entity_stages__missing_entity():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = [
        "entity-a.standardised",
        "entity-a.harmonised",
        "entity-x",  # This entry is missing so should be returned in the error message
        "entity-c.curated",
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        with pytest.raises(ValueError) as e:
            inst = DependencyResolver()
            inst.config = config
            inst._preprocess_entity_stages(entity_stages)
            assert "entity-x" in e.value.message


def test__dependency_resolver__preprocess_entity_stages__exploded_entry_drops_duplicates():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = [
        "entity-a.standardised",
        "entity-a",  # Exploded, but also has an explicit entry that should be removed because it is a duplicate
        "entity-b.curated",
        "entity-c.curated",
    ]
    expected_entity_stages = ["entity-a.standardised", "entity-a.harmonised", "entity-b.curated", "entity-c.curated"]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        inst = DependencyResolver()
        inst.config = config
        actual_output = inst._preprocess_entity_stages(entity_stages)
        assert set(actual_output) == set(expected_entity_stages)
        assert len(actual_output) == len(expected_entity_stages)


def test__dependency_resolver__preprocess_entity_stages__missing_entity_stage():
    config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
                "harmonised": {"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]},
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {"curated": {"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}},
        }
    }
    entity_stages = ["entity-a.standardised", "entity-a.harmonised", "entity-x.curated", "entity-c.y"]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        with pytest.raises(ValueError) as e:
            inst = DependencyResolver()
            inst.config = config
            inst._preprocess_entity_stages(entity_stages)
            assert "entity-x.curated" in e.value.message
            assert "entity-c.y" in e.value.message


def test__dependency_resolver__validate():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {"etl_process": "A-S", "kwargs": {"entity_name": "A", "etl_process_name": "AS"}, "depends_on": []},
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A", "etl_process_name": "AH"},
                    "depends_on": ["entity-a.standardised"],
                },
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B", "etl_process_name": "BS"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A", "etl_process_name": "BC"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A", "etl_process_name": "CC"}, "depends_on": ["entity-b.curated"]}
            },
        }
    }
    try:
        DependencyResolver.validate_config(example_config)
    except Exception as e:
        pytest.fail(f"Expected DependencyResolver.validate_config to not raise an error, but raised {e}")


def test__dependency_resolver__validate__with_invalid_structure():
    example_config = {"a": "b"}
    with pytest.raises(ValidationError):
        DependencyResolver.validate_config(example_config)


def test__dependency_resolver__validate__with_cycle():
    example_config = {
        "entities": {
            "entity-a": {
                "standardised": {
                    "etl_process": "A-S",
                    "kwargs": {"entity_name": "A", "etl_process_name": "AS"},
                    "depends_on": ["entity-c.curated"],
                },  # Cycle starts here
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A", "etl_process_name": "AH"},
                    "depends_on": ["entity-a.standardised"],
                },
            },
            "entity-b": {
                "standardised": {"etl_process": "B-S", "kwargs": {"entity_name": "B", "etl_process_name": "BS"}, "depends_on": []},
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A", "etl_process_name": "BC"}, "depends_on": ["entity-a.harmonised"]},
            },
            "entity-c": {
                "curated": {"etl_process": "A", "kwargs": {"entity_name": "A", "etl_process_name": "CC"}, "depends_on": ["entity-b.curated"]}
            },
        }
    }
    with pytest.raises(CycleError):
        DependencyResolver.validate_config(example_config)
