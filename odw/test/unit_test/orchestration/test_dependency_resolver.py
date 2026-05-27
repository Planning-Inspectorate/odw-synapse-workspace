from odw.core.orchestration.dependency_resolver import DependencyResolver
from odw.core.orchestration.orchestration_config import OrchestrationConfig
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
                    "entity_stage_name": "entity-a.standardised",
                },
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.standardised"],
                    "entity_stage_name": "entity-a.harmonised",
                },
            },
            "entity-b": {
                "standardised": {
                    "etl_process": "B-S",
                    "kwargs": {"entity_name": "B"},
                    "depends_on": [],
                    "entity_stage_name": "entity-b.standardised",
                },
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.harmonised"],
                    "entity_stage_name": "entity-b.curated",
                },
            },
            "entity-c": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-b.curated"],
                    "entity_stage_name": "entity-c.curated",
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
            {"entity_stage_name": "entity-a.standardised", "etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
            {"entity_stage_name": "entity-b.standardised", "etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
        ],
        [{"entity_stage_name": "entity-a.harmonised", "etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]}],
        [{"entity_stage_name": "entity-b.curated", "etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]}],
        [{"entity_stage_name": "entity-c.curated", "etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config).topological_sort()
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
        [{"entity_stage_name": "D.a", "name": "D", "depends_on": []}],
        [{"entity_stage_name": "C.a", "name": "C", "depends_on": ["D.a"]}, {"entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]}],
        [{"entity_stage_name": "B.a", "name": "B", "depends_on": ["C.a"]}],
        [
            {"entity_stage_name": "A.a", "name": "A", "depends_on": ["B.a", "C.a"]},
            {"entity_stage_name": "F.a", "name": "F", "depends_on": ["B.a", "D.a"]},
        ],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config).topological_sort()
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
            DependencyResolver(example_config).topological_sort()


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
                    "entity_stage_name": "entity-a.standardised",
                },
                "harmonised": {
                    "etl_process": "A-H",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.standardised"],
                    "entity_stage_name": "entity-a.harmonised",
                },
            },
            "entity-b": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-a.harmonised"],
                    "entity_stage_name": "entity-b.curated",
                },
            },
            "entity-c": {
                "curated": {
                    "etl_process": "A",
                    "kwargs": {"entity_name": "A"},
                    "depends_on": ["entity-b.curated"],
                    "entity_stage_name": "entity-c.curated",
                }
            },
        }
    }
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config).filter_irrelevant_dependencies_from_config(required_entity_stages)
        assert actual_output == expected_output


def test__dependency_resolver__filter_already_executed_entity_stages():
    topological_order = [
        [{"entity_stage_name": "D.a", "name": "D", "depends_on": []}],
        [{"entity_stage_name": "C.a", "name": "C", "depends_on": ["D.a"]}, {"entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]}],
        [{"entity_stage_name": "B.a", "name": "B", "depends_on": ["C.a"]}],
        [
            {"entity_stage_name": "A.a", "name": "A", "depends_on": ["B.a", "C.a"]},
            {"entity_stage_name": "F.a", "name": "F", "depends_on": ["B.a", "D.a"]},
        ],
    ]
    execution_details = [
        ("D", "a", "Succeeded"),
        ("C", "a", "Failed"),
        ("E", "a", "Canceled"),
        ("B", "a", "Cancelled"),
        ("A", "a", "Succeeded"),
        ("F", "a", "Canceled"),
    ]
    expected_output = [
        [{"entity_stage_name": "C.a", "name": "C", "depends_on": ["D.a"]}, {"entity_stage_name": "E.a", "name": "E", "depends_on": ["D.a"]}],
        [{"entity_stage_name": "B.a", "name": "B", "depends_on": ["C.a"]}],
        [{"entity_stage_name": "F.a", "name": "F", "depends_on": ["B.a", "D.a"]}],
    ]
    with mock.patch.object(DependencyResolver, "__init__", return_value=None):
        actual_output = DependencyResolver(None).filter_already_executed_entity_stages(topological_order, execution_details)
        assert actual_output == expected_output


def test__dependency_resolver__generate_stages_to_run():
    with (
        mock.patch.object(DependencyResolver, "__init__", return_value=None),
        mock.patch.object(DependencyResolver, "filter_irrelevant_dependencies_from_config", return_value="A"),
        mock.patch.object(DependencyResolver, "topological_sort", return_value="B"),
        mock.patch.object(DependencyResolver, "filter_already_executed_entity_stages", return_value="C"),
    ):
        dr = DependencyResolver(None)
        config = "X"
        dr.config = config
        entity_stages = "Y"
        execution_details = "Z"
        result = dr.generate_stages_to_run(entity_stages, execution_details)
        DependencyResolver.filter_irrelevant_dependencies_from_config.assert_called_once_with(entity_stages)
        DependencyResolver.topological_sort.assert_called_once_with("A")
        DependencyResolver.filter_already_executed_entity_stages.assert_called_once_with("B", execution_details)
        assert result == DependencyResolver.filter_already_executed_entity_stages.return_value
