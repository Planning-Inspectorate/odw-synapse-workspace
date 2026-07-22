from typing import List

from pipelines.scripts.packages.spark_pool_configuration import SparkPoolConfiguration
from pipelines.scripts.packages.synapse_types import BigDataPoolResourceInfo, LibraryInfo


ENV = "test"
ODW_PACKAGE_NAME = "odw-1.0.0-newcommit-py3-none-any.whl"


def _library(name: str) -> LibraryInfo:
    return LibraryInfo(
        name=name,
        path=f"pins-synw-odw-{ENV}-uks/libraries/{name}",
        containerName="prep",
        uploadedTimestamp="0001-01-01T00:00:00+00:00",
        type="whl" if name.endswith("whl") else "jar",
    )


def _config(custom_libraries: List[LibraryInfo]) -> BigDataPoolResourceInfo:
    return BigDataPoolResourceInfo(
        properties={"customLibraries": custom_libraries}
    )


def test_no_change_when_odw_package_present_and_no_local_packages():
    existing = [_library(ODW_PACKAGE_NAME)]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages=set(),
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is False
    assert libraries == existing


def test_update_when_odw_package_missing():
    existing = [_library("odw-1.0.0-oldcommit-py3-none-any.whl")]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages=set(),
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is True
    assert [library["name"] for library in libraries] == [ODW_PACKAGE_NAME]
    assert libraries[0]["path"] == f"pins-synw-odw-{ENV}-uks/libraries/{ODW_PACKAGE_NAME}"
    assert libraries[0]["type"] == "whl"


def test_update_when_multiple_odw_packages_present():
    existing = [
        _library(ODW_PACKAGE_NAME),
        _library("odw-1.0.0-oldcommit-py3-none-any.whl"),
    ]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages=set(),
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is True
    assert [library["name"] for library in libraries] == [ODW_PACKAGE_NAME]


def test_keeps_non_odw_package_that_is_defined_locally():
    kept = _library("mypackage.whl")
    existing = [_library(ODW_PACKAGE_NAME), kept]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages={"mypackage.whl"},
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is False
    assert kept in libraries
    assert {library["name"] for library in libraries} == {ODW_PACKAGE_NAME, "mypackage.whl"}


def test_drops_non_odw_package_not_defined_locally():
    stale = _library("stale.whl")
    existing = [_library(ODW_PACKAGE_NAME), stale]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages=set(),
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is True
    assert stale not in libraries
    assert {library["name"] for library in libraries} == {ODW_PACKAGE_NAME}


def test_adds_new_local_packages_with_correct_type():
    existing = [_library(ODW_PACKAGE_NAME)]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages={"newlib.whl", "otherlib.jar"},
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is True
    libraries_by_name = {library["name"]: library for library in libraries}
    assert set(libraries_by_name) == {ODW_PACKAGE_NAME, "newlib.whl", "otherlib.jar"}
    assert libraries_by_name["newlib.whl"]["type"] == "whl"
    assert libraries_by_name["otherlib.jar"]["type"] == "jar"
    assert libraries_by_name["newlib.whl"]["path"] == f"pins-synw-odw-{ENV}-uks/libraries/newlib.whl"


def test_does_not_duplicate_local_package_already_present():
    existing = [_library(ODW_PACKAGE_NAME), _library("mypackage.whl")]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages={"mypackage.whl"},
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    names = [library["name"] for library in libraries]
    assert names.count("mypackage.whl") == 1
    assert should_update is False

def test_ignores_order():
    existing = [ _library("mypackage.whl"), _library("mypackage2.whl"), _library(ODW_PACKAGE_NAME), _library("mypackage3.whl")]
    config = SparkPoolConfiguration(_config(existing))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages={"mypackage2.whl", "mypackage.whl", "mypackage3.whl"},
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )
    assert should_update is False


def test_empty_configuration_adds_odw_package():
    config = SparkPoolConfiguration(_config([]))

    should_update, libraries = config.get_updated_custom_libraries(
        local_packages=set(),
        odw_package_name=ODW_PACKAGE_NAME,
        env=ENV,
    )

    assert should_update is True
    assert [library["name"] for library in libraries] == [ODW_PACKAGE_NAME]

