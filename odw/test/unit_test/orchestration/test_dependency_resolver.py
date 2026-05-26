from odw.core.orchestration.dependency_resolver import DependencyResolver
from odw.core.orchestration.orchestration_config import OrchestrationConfig
from graphlib import CycleError
import pytest
import mock


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
            {"etl_process": "A-S", "kwargs": {"entity_name": "A"}, "depends_on": []},
            {"etl_process": "B-S", "kwargs": {"entity_name": "B"}, "depends_on": []},
        ],
        [{"etl_process": "A-H", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.standardised"]}],
        [{"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-a.harmonised"]}],
        [{"etl_process": "A", "kwargs": {"entity_name": "A"}, "depends_on": ["entity-b.curated"]}],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config).topological_sort()
        assert expected_output == actual_output


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
        [{"name": "D", "depends_on": []}],
        [{"name": "C", "depends_on": ["D.a"]}, {"name": "E", "depends_on": ["D.a"]}],
        [{"name": "B", "depends_on": ["C.a"]}],
        [{"name": "A", "depends_on": ["B.a", "C.a"]}, {"name": "F", "depends_on": ["B.a", "D.a"]}],
    ]
    with mock.patch.object(OrchestrationConfig, "model_validate", return_value=None):
        actual_output = DependencyResolver(example_config).topological_sort()
        assert expected_output == actual_output


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
