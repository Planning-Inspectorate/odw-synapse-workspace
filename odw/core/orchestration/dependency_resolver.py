from odw.core.orchestration.orchestration_config import OrchestrationConfig
from graphlib import TopologicalSorter
from typing import List, Dict, Any, Union


class DependencyResolver:
    def __init__(self, config: Dict[str, Any]):
        """
        :param config Dict: The orchestration config
        """
        if not isinstance(config, dict):
            raise ValueError(f"Expected the given config to be a dictionary, but was of type {type(config)}")
        self.config = config
        # Validate the config to ensure it does not have any missing or unexpected properties
        OrchestrationConfig.model_validate(self.config)

    def topological_sort(self):
        """
        Calculate a group topological order based on the loaded config. Stages in the config that could be run in parallel are grouped together

        ```
        sorter = DependencyResolver(
            {A: [B, C], B: [C], C: [D], D: [], E: [D], F: [B, D]}
        )
        ordered_groups = sorter.topological_sort()
        >> [[D], [C, E], [B], [A, F]]
        ```
        """
        entities: Dict[str, Dict[str, Dict[str, Union[Any, List[Any]]]]] = self.config.get("entities", dict())
        entity_stages = {f"{entity_name}.{stage_name}": stage for entity_name, entity in entities.items() for stage_name, stage in entity.items()}
        dependency_map = {entity_stage_name: tuple(entity["depends_on"]) for entity_stage_name, entity in entity_stages.items()}
        sorter = TopologicalSorter(dependency_map)
        sorter.prepare()
        ordered_groups = []
        while sorter.is_active():
            ready_nodes = tuple(sorter.get_ready())
            ordered_groups.append(ready_nodes)
            for node in ready_nodes:
                sorter.done(node)
        return [[entity_stages[entity_stage_name] for entity_stage_name in group] for group in ordered_groups]
