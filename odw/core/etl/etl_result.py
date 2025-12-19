from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import Type, ClassVar
from datetime import datetime


class ETLResult(BaseModel, ABC):
    class ETLResultMetadata(BaseModel):
        start_execution_time: datetime
        end_execution_time: datetime
        exception: Exception = None
        table_name: str
        insert_count: int
        update_count: int
        delete_count: int
        activity_type: str
        duration_seconds: str
        status_code: str
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


class ETLSuccessResult(ETLResult):
    """
    For successful ETLProcesses
    """
    outcome: ClassVar[str] = "Succeeded"
    status_code: 200
    metadata: ETLResult.ETLResultMetadata


class ETLFailResult(ETLResult):
    """
    For unsuccessful ETLProcesses
    """
    outcome: ClassVar[str] = "Failed"
    status_code: 500
    exception: Exception


class ETLResultFactory():
    """
    Automatically generate an ETLResult class from an outcome string
    """
    ETLFailResult.outcome
    @classmethod
    def get(cls, result_outcome: str) -> Type[ETLResult]:
        result_map = {
            result_class.outcome: result_class
            for result_class in (
                ETLSuccessResult,
                ETLFailResult
            )
        }
        if result_outcome not in result_map:
            raise ValueError(f"No ETLResult could be found for outcome '{result_outcome}'")
        return result_map[result_outcome]
