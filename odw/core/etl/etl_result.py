from pydantic import BaseModel
from abc import ABC, abstractmethod
from typing import Type, ClassVar


class ETLResult(BaseModel, ABC):
    """
    Holds the details of the executionn of an ETLProcess, for use in logging
    """
    @property
    @abstractmethod
    def outcome(self) -> str:
        """The outcome of the ETL process"""


class ETLSuccessResult(ETLResult):
    """
    For successful ETLProcesses
    """
    outcome: ClassVar[str] = "Succeeded"


class ETLFailResult(ETLResult):
    """
    For unsuccessful ETLProcesses
    """
    outcome: ClassVar[str] = "Failed"


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
