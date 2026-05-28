from pydantic import BaseModel, validator
from typing import List, Dict, Any


class OrchestrationConfig(BaseModel):
    class EntityStage(BaseModel):
        kwargs: Dict[str, Any]
        depends_on: List[str]

        @validator("kwargs")
        def validate_kwargs(cls, v):
            if "entity_stage_name" not in v:
                raise ValueError("kwargs must contain a 'entity_stage_name' entry")
            return v

    entities: Dict[str, Dict[str, EntityStage]]
