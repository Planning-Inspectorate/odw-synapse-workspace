from pydantic import BaseModel, Field
from abc import ABC, abstractmethod
from typing import Type, ClassVar, Optional
from datetime import datetime


class ETLResult(BaseModel, ABC):
    class ETLResultMetadata(BaseModel):
        start_execution_time: datetime
        end_execution_time: datetime
        exception: Optional[str] = None
        exception_trace: Optional[str] = None
        table_name: Optional[str] = None
        insert_count: int = Field(default_factory=0)
        update_count: int = Field(default_factory=0)
        delete_count: int = Field(default_factory=0)
        activity_type: str
        duration_seconds: float

    """
    Holds the details of the executionn of an ETLProcess, for use in logging
    """

    @property
    @abstractmethod
    def outcome(self) -> str:
        """The outcome of the ETL process"""

    @property
    @abstractmethod
    def status_code(self) -> int:
        """The status code of the ETL process"""

    metadata: ETLResultMetadata


class ETLSuccessResult(ETLResult):
    """
    For successful ETLProcesses
    """

    outcome: ClassVar[str] = "Succeeded"
    status_code: ClassVar[int] = 200


class ETLFailResult(ETLResult):
    """
    For unsuccessful ETLProcesses
    """

    outcome: ClassVar[str] = "Failed"
    status_code: ClassVar[int] = 500


class ETLResultFactory:
    """
    Automatically generate an ETLResult class from an outcome string
    """

    ETLFailResult.outcome

    @classmethod
    def get(cls, result_outcome: str) -> Type[ETLResult]:
        result_map = {result_class.outcome: result_class for result_class in (ETLSuccessResult, ETLFailResult)}
        if result_outcome not in result_map:
            raise ValueError(f"No ETLResult could be found for outcome '{result_outcome}'")
        return result_map[result_outcome]
