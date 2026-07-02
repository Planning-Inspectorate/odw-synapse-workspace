from pydantic import BaseModel, field_validator
from typing import List, Dict, Any


class OrchestrationConfig(BaseModel):
    class EntityStage(BaseModel):
        kwargs: Dict[str, Any]
        depends_on: List[str]

        @field_validator("kwargs")
        def validate_kwargs(cls, v):
            if "etl_process_name" not in v:
                raise ValueError("kwargs must contain a 'etl_process_name' entry")
            return v

    entities: Dict[str, Dict[str, EntityStage]]


class PreprocessedOrchestrationEntry(BaseModel):
    kwargs: Dict[str, Any]
    depends_on: List[str]
    orchestration_entity_stage_name: str
    orchestration_entity_name: str
    orchestration_stage_name: str
