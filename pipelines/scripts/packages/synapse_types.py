"""
    Type definitions for the Synapse Big Data Pool (spark pool) "Create Or Update" request body.

    Based on:
    https://learn.microsoft.com/en-us/rest/api/synapse/resourcemanager/big-data-pools/create-or-update?view=rest-synapse-resourcemanager-2021-06-01&tabs=HTTP#request-body
"""
from typing import Dict, List, Literal, TypedDict


# Enumerations defined by the API
NodeSize = Literal["None", "Small", "Medium", "Large", "XLarge", "XXLarge", "XXXLarge"]
NodeSizeFamily = Literal["None", "MemoryOptimized", "HardwareAcceleratedFPGA", "HardwareAcceleratedGPU"]
ConfigurationType = Literal["File", "Artifact"]


class AutoPauseProperties(TypedDict, total=False):
    """Auto-pausing properties of a Big Data pool powered by Apache Spark"""
    delayInMinutes: int
    enabled: bool


class AutoScaleProperties(TypedDict, total=False):
    """Auto-scaling properties of a Big Data pool powered by Apache Spark"""
    enabled: bool
    maxNodeCount: int
    minNodeCount: int


class DynamicExecutorAllocation(TypedDict, total=False):
    """Dynamic Executor Allocation Properties"""
    enabled: bool
    maxExecutors: int
    minExecutors: int


class LibraryRequirements(TypedDict, total=False):
    """Library requirements for a Big Data pool powered by Apache Spark"""
    content: str
    filename: str
    time: str | None


class SparkConfigProperties(TypedDict, total=False):
    """Spark pool configuration properties (SparkConfig Properties for a Big Data pool)"""
    configurationType: ConfigurationType
    content: str
    filename: str


class LibraryInfo(TypedDict, total=False):
    """Information about a library/package registered in the Synapse workspace"""
    containerName: str
    creatorId: str
    name: str
    path: str
    provisioningStatus: str
    type: str
    uploadedTimestamp: str


class BigDataPoolResourceProperties(TypedDict, total=False):
    """Properties of a Big Data pool powered by Apache Spark"""
    autoPause: AutoPauseProperties
    autoScale: AutoScaleProperties
    cacheSize: int
    customLibraries: List[LibraryInfo]
    defaultSparkLogFolder: str
    dynamicExecutorAllocation: DynamicExecutorAllocation
    isAutotuneEnabled: bool
    isComputeIsolationEnabled: bool
    libraryRequirements: LibraryRequirements | None
    nodeCount: int
    nodeSize: NodeSize
    nodeSizeFamily: NodeSizeFamily
    provisioningState: str
    sessionLevelPackagesEnabled: bool
    sparkConfigProperties: SparkConfigProperties
    sparkEventsFolder: str
    sparkVersion: str


class BigDataPoolResourceInfo(TypedDict, total=False):
    """
        A Big Data pool (spark pool) resource, used as the request body for the
        Big Data Pools "Create Or Update" operation.

        Note: `location` is required by the API, while every other field is optional.
    """
    id: str
    location: str
    name: str
    properties: BigDataPoolResourceProperties
    tags: Dict[str, str]
    type: str

