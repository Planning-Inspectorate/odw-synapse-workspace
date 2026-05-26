from pydantic import BaseModel
from typing import List, Dict, Any


class OrchestrationConfig(BaseModel):
    class EntityStage(BaseModel):
        etl_process: str
        kwargs: Dict[str, Any]
        depends_on: List[str]

    entities: Dict[str, Dict[str, EntityStage]]
