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
        # Validate the config to ensure it does not have any missing or unexpected properties
        self.config = config
        OrchestrationConfig.model_validate(self.config)
        self._preprocess_config()

    def _preprocess_config(self):
        """
        Mutably add a `entity_stage_name` key to each entity stage in the config
        """
        entities: Dict[str, Dict[str, Dict[str, Union[Any, List[Any]]]]] = self.config.get("entities", dict())
        for entity_name, entity in entities.items():
            for stage_name, stage in entity.items():
                stage["entity_stage_name"] = f"{entity_name}.{stage_name}"

    def _topological_sort(self):
        """
        Calculate a group topological order based on the loaded config. Stages in the config that could be run in parallel are grouped together

        ```
        sorter = DependencyResolver(
            {A: [B, C], B: [C], C: [D], D: [], E: [D], F: [B, D]}
        )
        ordered_groups = sorter._topological_sort()
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

    def _filter_irrelevant_dependencies_from_config(self, entity_stages_to_keep: List[str] = []):
        """
        Mutably filter down the config so that it only includes the direct and indirect dependends of the given stages
        """
        if not entity_stages_to_keep:
            # If no stages specified, keep all of them and skip filtering
            return
        entities: Dict[str, Dict[str, Dict[str, Union[Any, List[Any]]]]] = self.config.get("entities", dict())
        entity_stages = {f"{entity_name}.{stage_name}": stage for entity_name, entity in entities.items() for stage_name, stage in entity.items()}
        visited = set()
        unvisited = [x for x in entity_stages_to_keep]
        while unvisited:
            current = unvisited.pop()
            unvisited.extend(entity_stages[current].get("depends_on", []))
            visited.add(current)
        cleaned_entities = dict()
        for entity_stage_name in visited:
            entity_stage_name_split = entity_stage_name.split(".")
            entity_name = entity_stage_name_split[0]
            stage_name = entity_stage_name_split[1]
            entity_stage = entity_stages[entity_stage_name]
            if entity_name not in cleaned_entities:
                cleaned_entities[entity_name] = {stage_name: entity_stage}
            else:
                cleaned_entities[entity_name][stage_name] = entity_stage
        self.config["entities"] = cleaned_entities

    def filter_already_executed_entity_stages(self, topological_order_groups: List[List[Dict[str, Any]]], execution_details: List[Dict[str, Any]]):
        """
        Filter out entity stages from the topological order than have already veen marked as completed by the pipeline
        """
        # Might need to change this depending on how the data is structured
        entity_stage_execution_details = {
            f"{entity_name}.{stage_name}" for entity_name, stage_name, execution_status in execution_details if execution_status != "Succeeded"
        }
        cleaned_groups = [
            [entity for entity in group if entity["entity_stage_name"] in entity_stage_execution_details] for group in topological_order_groups
        ]
        return [x for x in cleaned_groups if x]

    def generate_stages_to_run(self, entity_stages: List[str], execution_details: Dict[str, Any]):
        """
        Filter down the config so that only relevent entities are executed and group them into batches that can run concurrently
        """
        self._filter_irrelevant_dependencies_from_config(entity_stages)
        groups = self._topological_sort()
        return self.filter_already_executed_entity_stages(groups, execution_details)
