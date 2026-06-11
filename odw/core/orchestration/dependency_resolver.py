from odw.core.orchestration.orchestration_config import OrchestrationConfig
from graphlib import TopologicalSorter
from typing import List, Dict, Any, Union
import json


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

    def _preprocess_entity_stages(self, entity_stages: List[str]):
        delimiter = "."
        # Get the entries that have too many delimiters
        invalid_entries = [str(entry) for entry in entity_stages if len(str(entry).split(delimiter)) > 2]
        if invalid_entries:
            raise ValueError(
                f"Each entry of the entity_stages must have the form <entity_name.stage_name>. The following entries were invalid: {json.dumps(invalid_entries, indent=4)}"
            )
        # Get the entries that have exactly one delimiter
        valid_entries = [entry for entry in entity_stages if len(entry.split(delimiter)) == 2]
        # Get the entries that do not have a delimiter - element after the delimiter can be inferred from the config
        entities_to_explode = [entry for entry in entity_stages if len(entry.split(delimiter)) == 1]
        missing_entities = [entity for entity in entities_to_explode if entity not in self.config.get("entities", dict())]
        if missing_entities:
            raise ValueError(
                f"The following entities were provided but could not be found in the config file: {json.dumps(missing_entities, indent=4)}"
            )
        # Generate new groups of entity_stages by extracting all stages for each entity
        exploded_entries = [
            [f"{entity}.{stage}" for stage in self.config.get("entities", dict()).get(entity, dict()).keys()] for entity in entities_to_explode
        ]
        # Combine the valid entries with the exploded entries
        flattened_exploded_entries = [y for x in exploded_entries for y in x]
        entries = list(set(valid_entries + flattened_exploded_entries))
        missing_entity_stages = [
            entity_stage
            for entity_stage in entries
            if not self.config.get("entities", dict()).get(entity_stage.split(delimiter)[0], dict()).get(entity_stage.split(delimiter)[1], None)
        ]
        if missing_entity_stages:
            raise ValueError(f"The following entity_stages could not be found in the config: {json.dumps(missing_entity_stages, indent=4)}")
        return entries

    def _preprocess_config(self):
        """
        Mutably add a `orchestration_entity_stage_name` key to each entity stage in the config
        """
        entities: Dict[str, Dict[str, Dict[str, Union[Any, List[Any]]]]] = self.config.get("entities", dict())
        for entity_name, entity in entities.items():
            for stage_name, stage in entity.items():
                stage["orchestration_entity_stage_name"] = f"{entity_name}.{stage_name}"
                stage["orchestration_entity_name"] = entity_name
                stage["orchestration_stage_name"] = stage_name

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
        dependency_map = {
            orchestration_entity_stage_name: tuple(entity["depends_on"]) for orchestration_entity_stage_name, entity in entity_stages.items()
        }
        sorter = TopologicalSorter(dependency_map)
        sorter.prepare()
        ordered_groups = []
        while sorter.is_active():
            ready_nodes = tuple(sorter.get_ready())
            ordered_groups.append(ready_nodes)
            for node in ready_nodes:
                sorter.done(node)
        return [[entity_stages[orchestration_entity_stage_name] for orchestration_entity_stage_name in group] for group in ordered_groups]

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
        for orchestration_entity_stage_name in visited:
            entity_stage_name_split = orchestration_entity_stage_name.split(".")
            if len(entity_stage_name_split) != 2:
                raise RuntimeError(
                    f"orchestration_entity_stage_name should have the form entity_name.stage_name but had value '{orchestration_entity_stage_name}'"
                )
            entity_name = entity_stage_name_split[0]
            stage_name = entity_stage_name_split[1]
            entity_stage = entity_stages[orchestration_entity_stage_name]
            if entity_name not in cleaned_entities:
                cleaned_entities[entity_name] = {stage_name: entity_stage}
            else:
                cleaned_entities[entity_name][stage_name] = entity_stage
        self.config["entities"] = cleaned_entities

    def filter_already_executed_entity_stages(self, topological_order_groups: List[List[Dict[str, Any]]], execution_details: List[Dict[str, Any]]):
        """
        Filter out entity stages from the topological order than have already veen marked as completed by the pipeline
        """
        if not execution_details:
            # Return all entity stages if no execution details could be found
            return topological_order_groups
        # A run is considered "failed" if it is not successful. Entries that were not executed may not appear in execution_details
        successful_executions = {f"{row['entity_name']}.{row['stage_name']}" for row in execution_details if row["successful"]}
        cleaned_groups = [
            [entity for entity in group if entity["orchestration_entity_stage_name"] not in successful_executions]
            for group in topological_order_groups
        ]
        return [x for x in cleaned_groups if x]

    @classmethod
    def filter_entity_stages_with_failed_dependencies(cls, group: List[Dict[str, Any]], execution_details: List[Dict[str, Any]]):
        """
        Filter out entity stages from a group that have had any of its dependencies fail
        """
        if not execution_details:
            return group
        failed_executions = {f"{row['entity_name']}.{row['stage_name']}" for row in execution_details if not row["successful"]}
        return [
            entity_stage for entity_stage in group if not any(dependency in failed_executions for dependency in entity_stage.get("depends_on", []))
        ]

    def generate_stages_to_run(self, entity_stages: List[str], execution_details: Dict[str, Any]):
        """
        Filter down the config so that only relevent entities are executed and group them into batches that can run concurrently
        """
        cleaned_entity_stages = self._preprocess_entity_stages(entity_stages)
        self._filter_irrelevant_dependencies_from_config(cleaned_entity_stages)
        groups = self._topological_sort()
        return self.filter_already_executed_entity_stages(groups, execution_details)
