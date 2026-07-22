from typing import List

import logging

import mock
import pytest

from pipelines.scripts.deploy_packages import ODWPackageDeployer

@pytest.fixture(autouse=True)
def _stop_started_patches():
    yield
    mock.patch.stopall()

# ---------------------------------------------------------------------------
# get_odw_packages_bound_to_extra_spark_pools
# ---------------------------------------------------------------------------

class TestGetOdwPackagesBoundToExtraSparkPools:
    # noinspection method-may-be-static
    def _deployer(self, spark_pools, target_spark_pools: List[str]):
        """
        Build an ODWPackageDeployer with a mocked workspace manager that returns the
        given spark pools, and TARGET_SPARK_POOLS patched to the given collection.
        """
        workspace_manager = mock.MagicMock()
        workspace_manager.get_all_spark_pools.return_value = spark_pools
        deployer = ODWPackageDeployer(workspace_manager, env="mock_env", odw_package_name="odw-test.whl")
        mock.patch.object(ODWPackageDeployer, "TARGET_SPARK_POOLS", target_spark_pools).start()
        return deployer, workspace_manager


    def test_returns_only_odw_packages_bound_to_extra_pools(self):
        """
            Given spark pools with odw packages bound to them
            When get_odw_packages_bound_to_extra_spark_pools is called
            Then only odw packages bound to pools outside TARGET_SPARK_POOLS are returned
        """
        mock_spark_pools = [
            {
                "name": "someOtherPoolA",
                "properties": {
                    "customLibraries": [
                        {"name": "odwPackageA"},
                        {"name": "odwPackageB"},
                    ]
                },
            },
            {
                "name": "someOtherPoolB",
                "properties": {
                    "customLibraries": [
                        {"name": "odwPackageC"},
                    ]
                },
            },
            {
                "name": "mockMainPool",
                "properties": {
                    "customLibraries": [
                        {"name": "odwPackageD"},
                        {"name": "odwPackageE"},
                    ]
                },
            },
        ]
        deployer, _ = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == {"odwPackageA", "odwPackageB", "odwPackageC"}


    def test_ignores_non_odw_packages_on_extra_pools(self):
        """
            Only packages whose name starts with 'odw' should be returned
        """
        mock_spark_pools = [
            {
                "name": "someOtherPool",
                "properties": {
                    "customLibraries": [
                        {"name": "odwPackageA"},
                        {"name": "notOdwPackage"},
                        {"name": "anotherlib.jar"},
                    ]
                },
            },
        ]
        deployer, _ = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == {"odwPackageA"}


    def test_excludes_packages_bound_only_to_target_pools(self):
        """
            odw packages that are bound solely to pools in TARGET_SPARK_POOLS are not returned
        """
        mock_spark_pools = [
            {
                "name": "mockMainPool",
                "properties": {
                    "customLibraries": [
                        {"name": "odwPackageA"},
                    ]
                },
            },
        ]
        deployer, _ = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == set()


    def test_deduplicates_packages_across_extra_pools(self):
        """
            The same odw package bound to multiple extra pools appears once in the result
        """
        mock_spark_pools = [
            {
                "name": "someOtherPoolA",
                "properties": {"customLibraries": [{"name": "odwShared"}]},
            },
            {
                "name": "someOtherPoolB",
                "properties": {"customLibraries": [{"name": "odwShared"}]},
            },
        ]
        deployer, _ = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == {"odwShared"}


    def test_handles_pool_with_no_custom_libraries(self):
        """
            A pool without a 'customLibraries' key should be handled gracefully
        """
        mock_spark_pools = [
            {
                "name": "someOtherPoolA",
                "properties": {},
            },
            {
                "name": "someOtherPoolB",
                "properties": {"customLibraries": [{"name": "odwPackageA"}]},
            },
        ]
        deployer, _ = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == {"odwPackageA"}


    def test_returns_empty_set_when_no_extra_pools(self):
        """
            When every pool is a target pool, no extra packages are returned
        """
        mock_spark_pools = [
            {
                "name": "mockMainPool",
                "properties": {"customLibraries": [{"name": "odwPackageA"}]},
            },
        ]
        deployer, workspace_manager = self._deployer(mock_spark_pools, ["mockMainPool"])
        result = deployer.get_odw_packages_bound_to_extra_spark_pools()
        assert result == set()
        workspace_manager.get_all_spark_pools.assert_called_once_with()


# ---------------------------------------------------------------------------
# _get_requirements_modifications
# ---------------------------------------------------------------------------



class TestRequirementsModifications:
    REQUIREMENTS_FILE_NAME = "requirements.txt"
    POOL_WITH_REQUIREMENTS = "pinssynspodw35"  # maps to requirements.txt
    POOL_NOT_IN_MAP = "someOtherPool"

    # noinspection method-may-be-static
    def _deployer(self):
        return ODWPackageDeployer(mock.MagicMock(), env="mock_env", odw_package_name="odw-test.whl")
    
    def test_requirements_raises_when_properties_missing(self):
        deployer = self._deployer()

        with pytest.raises(ValueError):
            deployer._get_requirements_modifications(self.POOL_WITH_REQUIREMENTS, {})


    def test_requirements_unmanaged_pool_with_workspace_requirements_is_modified(self):
        """
            A pool not in the requirements map that still has workspace requirements
            should be flagged as modified, with empty requirements to clear it.
        """
        deployer = self._deployer()
        spark_pool = {
            "properties": {
                "libraryRequirements": {"filename": "requirements.txt", "content": "pandas"}
            }
        }

        modified, new_requirements = deployer._get_requirements_modifications(self.POOL_NOT_IN_MAP, spark_pool)

        assert modified is True
        assert new_requirements == {}


    def test_requirements_unmanaged_pool_without_workspace_requirements_is_unmodified(self):
        deployer = self._deployer()
        spark_pool = {"properties": {}}

        modified, new_requirements = deployer._get_requirements_modifications(self.POOL_NOT_IN_MAP, spark_pool)

        assert modified is False
        assert new_requirements == {}


    def test_requirements_managed_pool_without_workspace_requirements_is_modified(self):
        """
            A pool that should have requirements but has none in the workspace is modified.
        """
        deployer = self._deployer()
        spark_pool = {"properties": {}}

        modified, new_requirements = deployer._get_requirements_modifications(self.POOL_WITH_REQUIREMENTS, spark_pool)

        assert modified is True
        assert new_requirements.get("content", "").startswith("# Spark 3.5 library list can be found here")


    def test_requirements_managed_pool_matching_content_is_unmodified(self):
        deployer = self._deployer()
        file_content = "pandas==2.0.0\nnumpy==1.26.0\n"
        spark_pool = {
            "properties": {
                "libraryRequirements": {
                    "filename": self.REQUIREMENTS_FILE_NAME,
                    "content": file_content,
                    "time": "2025-01-01T00:00:00.000000Z",  # extra key, should be ignored
                }
            }
        }

        with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
            modified, new_requirements = deployer._get_requirements_modifications(self.POOL_WITH_REQUIREMENTS, spark_pool)

        assert modified is False
        assert new_requirements == {}


    def test_requirements_managed_pool_differing_content_is_modified(self):
        deployer = self._deployer()
        file_content = "pandas==2.0.0\nnumpy==1.26.0\n"
        workspace_content = "pandas==1.0.0\n"
        spark_pool = {
            "properties": {
                "libraryRequirements": {
                    "filename": self.REQUIREMENTS_FILE_NAME,
                    "content": workspace_content,
                }
            }
        }

        with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
            modified, new_requirements = deployer._get_requirements_modifications(self.POOL_WITH_REQUIREMENTS, spark_pool)

        assert modified is True
        assert new_requirements["content"] == file_content


    def test_requirements_managed_pool_ignores_carriage_returns(self):
        """
            Workspace content that only differs by carriage returns should not count as a modification.
        """
        deployer = self._deployer()
        file_content = "pandas==2.0.0\nnumpy==1.26.0\n"
        workspace_content = "pandas==2.0.0\r\nnumpy==1.26.0\r\n"
        spark_pool = {
            "properties": {
                "libraryRequirements": {
                    "filename": self.REQUIREMENTS_FILE_NAME,
                    "content": workspace_content,
                }
            }
        }

        with mock.patch("builtins.open", mock.mock_open(read_data=file_content)):
            modified, _ = deployer._get_requirements_modifications(self.POOL_WITH_REQUIREMENTS, spark_pool)

        assert modified is False



# ---------------------------------------------------------------------------
# generate_new_spark_pool_json
# ---------------------------------------------------------------------------

class TestGenerateNewSparkPoolJson:
    SPARK_POOL_NAME = "pinssynspodw35"

    # noinspection method-may-be-static
    def _deployer_with_pool(self, spark_pool):
        workspace_manager = mock.MagicMock()
        workspace_manager.get_spark_pool.return_value = spark_pool
        return ODWPackageDeployer(workspace_manager, env="mock_env", odw_package_name="odw-test.whl")

    # noinspection method-may-be-static
    def _patches(self, requirements_result, libraries_result):
        """
        Patch the collaborators of generate_new_spark_pool_json. Returns the three
        context managers to be entered by the caller.
        """
        return (
            mock.patch.object(
                ODWPackageDeployer,
                "_get_requirements_modifications",
                return_value=requirements_result,
            ),
            mock.patch(
                "pipelines.scripts.deploy_packages.get_local_workspace_packages",
                return_value=set(),
            ),
            mock.patch("pipelines.scripts.deploy_packages.SparkPoolConfiguration"),
        )

    def test_returns_none_when_nothing_modified(self):
        spark_pool = {"properties": {"customLibraries": []}}
        deployer = self._deployer_with_pool(spark_pool)
        requirements_patch, local_packages_patch, config_patch = self._patches((False, {}), None)

        with requirements_patch, local_packages_patch, config_patch as mock_config_cls:
            mock_config_cls.return_value.get_updated_custom_libraries.return_value = (False, [])
            result = deployer.generate_new_spark_pool_json(self.SPARK_POOL_NAME)

        assert result is None

    def test_updates_requirements_when_only_requirements_modified(self):
        spark_pool = {"properties": {"customLibraries": []}}
        new_requirements = {"filename": "requirements.txt", "content": "pandas"}
        deployer = self._deployer_with_pool(spark_pool)
        requirements_patch, local_packages_patch, config_patch = self._patches((True, new_requirements), None)

        with requirements_patch, local_packages_patch, config_patch as mock_config_cls:
            mock_config_cls.return_value.get_updated_custom_libraries.return_value = (False, [])
            result = deployer.generate_new_spark_pool_json(self.SPARK_POOL_NAME)

        assert result is spark_pool
        assert result["properties"]["libraryRequirements"] == new_requirements
        assert result["properties"]["customLibraries"] == []

    def test_updates_libraries_when_only_libraries_modified(self):
        spark_pool = {"properties": {"customLibraries": []}}
        new_libraries = [{"name": "odw-test.whl"}]
        deployer = self._deployer_with_pool(spark_pool)
        requirements_patch, local_packages_patch, config_patch = self._patches((False, {}), None)

        with requirements_patch, local_packages_patch, config_patch as mock_config_cls:
            mock_config_cls.return_value.get_updated_custom_libraries.return_value = (True, new_libraries)
            result = deployer.generate_new_spark_pool_json(self.SPARK_POOL_NAME)

        assert result is spark_pool
        assert result["properties"]["customLibraries"] == new_libraries
        assert "libraryRequirements" not in result["properties"]

    def test_updates_both_when_both_modified(self):
        spark_pool = {"properties": {"customLibraries": []}}
        new_requirements = {"filename": "requirements.txt", "content": "pandas"}
        new_libraries = [{"name": "odw-test.whl"}]
        deployer = self._deployer_with_pool(spark_pool)
        requirements_patch, local_packages_patch, config_patch = self._patches((True, new_requirements), None)

        with requirements_patch, local_packages_patch, config_patch as mock_config_cls:
            mock_config_cls.return_value.get_updated_custom_libraries.return_value = (True, new_libraries)
            result = deployer.generate_new_spark_pool_json(self.SPARK_POOL_NAME)

        assert result is spark_pool
        assert result["properties"]["libraryRequirements"] == new_requirements
        assert result["properties"]["customLibraries"] == new_libraries

    def test_fetches_spark_pool_by_name_from_workspace_manager(self):
        spark_pool = {"properties": {"customLibraries": []}}
        deployer = self._deployer_with_pool(spark_pool)
        requirements_patch, local_packages_patch, config_patch = self._patches((False, {}), None)

        with requirements_patch, local_packages_patch, config_patch as mock_config_cls:
            mock_config_cls.return_value.get_updated_custom_libraries.return_value = (False, [])
            deployer.generate_new_spark_pool_json(self.SPARK_POOL_NAME)

        deployer.workspace_manager.get_spark_pool.assert_called_once_with(self.SPARK_POOL_NAME)


# ---------------------------------------------------------------------------
# get_package_changes
# ---------------------------------------------------------------------------

class TestGetPackageChanges:
    ODW_PACKAGE_NAME = "odw-current.whl"

    def _deployer(self):
        return ODWPackageDeployer(mock.MagicMock(), env="mock_env", odw_package_name=self.ODW_PACKAGE_NAME)

    # noinspection method-may-be-static
    def _patches(self, package_names, odw_names, local_packages):
        workspace_packages = mock.MagicMock()
        workspace_packages.package_names = package_names
        workspace_packages.odw_names = odw_names
        return (
            mock.patch("pipelines.scripts.deploy_packages.extract_jar_files"),
            mock.patch(
                "pipelines.scripts.deploy_packages.get_workspace_packages",
                return_value=workspace_packages,
            ),
            mock.patch(
                "pipelines.scripts.deploy_packages.get_local_workspace_packages",
                return_value=local_packages,
            ),
        )

    def test_computes_add_remove_and_add_odw_flag(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names={"keep.whl", "stale.whl"},
            odw_names={"odw-current.whl", "odw-old.whl", "odw-extra.whl"},
            local_packages={"keep.whl", "new.whl"},
        )

        with jar_patch, workspace_patch, local_patch:
            packages_to_add, packages_to_remove, add_odw_package = deployer.get_package_changes({"odw-extra.whl"})

        assert packages_to_add == {"new.whl"}
        assert packages_to_remove == {"stale.whl", "odw-old.whl"}
        assert add_odw_package is False

    def test_odw_package_bound_to_extra_pool_is_not_removed(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names=set(),
            odw_names={"odw-extra.whl"},
            local_packages=set(),
        )

        with jar_patch, workspace_patch, local_patch:
            _, packages_to_remove, _ = deployer.get_package_changes({"odw-extra.whl"})

        assert "odw-extra.whl" not in packages_to_remove

    def test_current_odw_package_is_not_removed(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names=set(),
            odw_names={"odw-current.whl"},
            local_packages=set(),
        )

        with jar_patch, workspace_patch, local_patch:
            _, packages_to_remove, add_odw_package = deployer.get_package_changes(set())

        assert "odw-current.whl" not in packages_to_remove
        assert add_odw_package is False

    def test_add_odw_package_true_when_missing_from_workspace(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names=set(),
            odw_names={"odw-old.whl"},
            local_packages=set(),
        )

        with jar_patch, workspace_patch, local_patch:
            _, _, add_odw_package = deployer.get_package_changes(set())

        assert add_odw_package is True

    def test_no_changes_when_workspace_matches_local(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names={"keep.whl"},
            odw_names={"odw-current.whl"},
            local_packages={"keep.whl"},
        )

        with jar_patch, workspace_patch, local_patch:
            packages_to_add, packages_to_remove, add_odw_package = deployer.get_package_changes(set())

        assert packages_to_add == set()
        assert packages_to_remove == set()
        assert add_odw_package is False

    def test_runs_gradle_build_before_reading_packages(self):
        deployer = self._deployer()
        jar_patch, workspace_patch, local_patch = self._patches(
            package_names=set(),
            odw_names=set(),
            local_packages=set(),
        )

        with jar_patch as mock_extract_jar_files, workspace_patch, local_patch:
            deployer.get_package_changes(set())

        mock_extract_jar_files.assert_called_once_with()


# ---------------------------------------------------------------------------
# update_synapse_packages
# ---------------------------------------------------------------------------

class TestUpdateSynapsePackages:
    # noinspection method-may-be-static
    def _deployer(self, packages_to_add, packages_to_remove, add_odw_package, generate_side_effect):
        workspace_manager = mock.MagicMock()
        deployer = ODWPackageDeployer(workspace_manager, env="mock_env", odw_package_name="odw-current.whl")
        deployer.get_odw_packages_bound_to_extra_spark_pools = mock.MagicMock(return_value=set())
        deployer.get_package_changes = mock.MagicMock(
            return_value=(packages_to_add, packages_to_remove, add_odw_package)
        )
        deployer.generate_new_spark_pool_json = mock.MagicMock(side_effect=generate_side_effect)
        return deployer

    def test_dry_run_makes_no_mutating_calls(self):
        deployer = self._deployer(
            packages_to_add={"new.whl"},
            packages_to_remove={"stale.whl"},
            add_odw_package=True,
            generate_side_effect=lambda name: None,
        )

        deployer.update_synapse_packages(deploy=False)

        deployer.workspace_manager.upload_workspace_package.assert_not_called()
        deployer.workspace_manager.remove_workspace_package.assert_not_called()
        deployer.workspace_manager.update_spark_pool.assert_not_called()

    def test_dry_run_logs_changes(self, caplog):
        deployer = self._deployer(
            packages_to_add={"new.whl"},
            packages_to_remove={"stale.whl"},
            add_odw_package=True,
            generate_side_effect=lambda name: None,
        )

        with caplog.at_level(logging.INFO):
            deployer.update_synapse_packages(deploy=False)

        messages = list(record.getMessage() for record in caplog.records)
        assert "new.whl" in messages[0] # not the full message
        assert "stale.whl" in messages[1] # not the full message
        assert "dry run, not deploying changes" in messages

    def test_deploy_uploads_added_packages(self):
        deployer = self._deployer(
            packages_to_add={"new.whl"},
            packages_to_remove=set(),
            add_odw_package=False,
            generate_side_effect=lambda name: None,
        )

        deployer.update_synapse_packages(deploy=True)

        deployer.workspace_manager.upload_workspace_package.assert_called_once_with(
            "configuration/workspace-packages/new.whl"
        )

    def test_deploy_removes_stale_packages(self):
        deployer = self._deployer(
            packages_to_add=set(),
            packages_to_remove={"stale.whl"},
            add_odw_package=False,
            generate_side_effect=lambda name: None,
        )

        deployer.update_synapse_packages(deploy=True)

        deployer.workspace_manager.remove_workspace_package.assert_called_once_with("stale.whl")

    def test_deploy_uploads_odw_package_when_flagged(self):
        deployer = self._deployer(
            packages_to_add=set(),
            packages_to_remove=set(),
            add_odw_package=True,
            generate_side_effect=lambda name: None,
        )

        deployer.update_synapse_packages(deploy=True)
        # no other packages to add, so there is only one call
        deployer.workspace_manager.upload_workspace_package.assert_called_once_with("dist/odw-current.whl")

    def test_deploy_updates_only_modified_spark_pools(self):
        def generate_side_effect(name):
            return {"name": name, "updated": True} if name == "pinssynspodw35" else None

        deployer = self._deployer(
            packages_to_add=set(),
            packages_to_remove=set(),
            add_odw_package=False,
            generate_side_effect=generate_side_effect,
        )

        deployer.update_synapse_packages(deploy=True)

        deployer.workspace_manager.update_spark_pool.assert_called_once_with(
            "pinssynspodw35", {"name": "pinssynspodw35", "updated": True}
        )

    def test_deploy_does_not_update_spark_pools_when_no_changes(self):
        deployer = self._deployer(
            packages_to_add=set(),
            packages_to_remove=set(),
            add_odw_package=False,
            generate_side_effect=lambda name: None,
        )

        deployer.update_synapse_packages(deploy=True)

        deployer.workspace_manager.update_spark_pool.assert_not_called()

    def test_package_changes_uses_extra_pool_assignments(self):
        deployer = self._deployer(
            packages_to_add=set(),
            packages_to_remove=set(),
            add_odw_package=False,
            generate_side_effect=lambda name: None,
        )
        deployer.get_odw_packages_bound_to_extra_spark_pools = mock.MagicMock(
            return_value={"odw-extra.whl"}
        )

        deployer.update_synapse_packages(deploy=False)

        deployer.get_package_changes.assert_called_once_with({"odw-extra.whl"})


# ---------------------------------------------------------------------------
# update_synapse_packages check odw packages are not removed if in use by
# other pools
# ---------------------------------------------------------------------------
class TestUpdateSynapsePackagesOdwUnknownPoolUnmodified:
    ENV = "mock_env"
    NEW_ODW = "odw-new.whl"
    OLD_ODW = "odw-old.whl"
    KNOWN_POOL = "knownPool"
    UNKNOWN_POOL = "mySparkPool"
    REQUIREMENTS_FILE = "requirements.txt"
    REQUIREMENTS_CONTENT = "some-requirement==1.0.0\n"

    def _odw_library(self, name: str):
        return {
            "name": name,
            "path": f"pins-synw-odw-{self.ENV}-uks/libraries/{name}",
            "containerName": "prep",
            "uploadedTimestamp": "0001-01-01T00:00:00+00:00",
            "type": "whl",
        }

    def _known_pool(self):
        return {
            "name": self.KNOWN_POOL,
            "properties": {
                "customLibraries": [self._odw_library(self.OLD_ODW)],
                "libraryRequirements": {
                    "filename": self.REQUIREMENTS_FILE,
                    "content": self.REQUIREMENTS_CONTENT,
                },
            },
        }

    def _unknown_pool(self,):
        return {
            "name": self.UNKNOWN_POOL,
            "properties": {
                "customLibraries": [self._odw_library(self.OLD_ODW)],
            },
        }

    def _deployer(self):
        workspace_manager = mock.MagicMock()
        # get_all_spark_pools drives get_odw_packages_bound_to_extra_spark_pools
        workspace_manager.get_all_spark_pools.return_value = [self._known_pool(), self._unknown_pool()]
        # get_spark_pool drives generate_new_spark_pool_json (only called for known pools).
        workspace_manager.get_spark_pool.side_effect = self._get_spark_pool
        # The old odw package is the only package currently in the workspace
        workspace_manager.get_workspace_packages.return_value = [{"name": self.OLD_ODW}]
        deployer = ODWPackageDeployer(workspace_manager, env=self.ENV, odw_package_name=self.NEW_ODW)
        return deployer, workspace_manager

    def _get_spark_pool(self, name: str):
        if name == self.KNOWN_POOL:
            return self._known_pool()
        else:
            return self._unknown_pool()

    def _run(self, deployer):
        with (
            mock.patch.object(
                ODWPackageDeployer,
                "SPARK_POOL_REQUIREMENTS_MAP",
                {self.KNOWN_POOL: self.REQUIREMENTS_FILE},
            ),
            mock.patch.object(ODWPackageDeployer, "TARGET_SPARK_POOLS", [self.KNOWN_POOL]),
            mock.patch("pipelines.scripts.deploy_packages.extract_jar_files"),
            mock.patch(
                "pipelines.scripts.deploy_packages.get_local_workspace_packages",
                return_value=set(),
            ),
            mock.patch("builtins.open", mock.mock_open(read_data=self.REQUIREMENTS_CONTENT)),
        ):
            deployer.update_synapse_packages(deploy=True)

    def test_new_odw_uploaded_to_workspace(self):
        deployer, workspace_manager = self._deployer()

        self._run(deployer)

        assert workspace_manager.upload_workspace_package.call_count == 1
        workspace_manager.upload_workspace_package.assert_called_once_with(f"dist/{self.NEW_ODW}")

        # old package is not removed
        deployer.workspace_manager.remove_workspace_package.assert_not_called()

    def test_new_odw_bound_to_known_pool(self):
        deployer, workspace_manager = self._deployer()

        self._run(deployer)

        # only the known pool is updated
        workspace_manager.update_spark_pool.assert_called_once()
        pool_name, pool_json = workspace_manager.update_spark_pool.call_args.args
        assert pool_name == self.KNOWN_POOL
        library_names = {library["name"] for library in pool_json["properties"]["customLibraries"]}
        assert library_names == {self.NEW_ODW}
        assert self.OLD_ODW not in library_names

    def test_old_odw_left_in_workspace(self):
        deployer, workspace_manager = self._deployer()

        self._run(deployer)

        removed_packages = [call.args[0] for call in workspace_manager.remove_workspace_package.call_args_list]
        assert self.OLD_ODW not in removed_packages
        workspace_manager.remove_workspace_package.assert_not_called()
