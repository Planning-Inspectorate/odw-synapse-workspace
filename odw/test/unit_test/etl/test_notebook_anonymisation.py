"""
Tests for the anonymisation logic embedded in py_sb_raw_to_std and py_horizon_raw_to_std.

The notebooks contain global-scope functions that close over Synapse runtime variables
(spark, storage_account, logInfo).  These tests replicate each function in a controlled
scope, wiring in test doubles for those globals so the logic can be exercised without a
live Synapse environment.
"""

from unittest import mock
from odw.core.anonymisation.engine import AnonymisationEngine
from odw.core.anonymisation.config import load_config, AnonymisationConfig
from odw.core.util.util import Util
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_sb_df(spark):
    return spark.createDataFrame(
        [
            {"EmployeeID": "E1", "full_name": "Alice Smith", "emailAddress": "alice@example.com"},
            {"EmployeeID": "E2", "full_name": "Bob Jones", "emailAddress": "bob@example.com"},
        ]
    )


def _anon_sb_df(spark):
    return spark.createDataFrame(
        [
            {"EmployeeID": "E1", "full_name": "REDACTED", "emailAddress": "hash_alice"},
            {"EmployeeID": "E2", "full_name": "REDACTED", "emailAddress": "hash_bob"},
        ]
    )


def _raw_horizon_df(spark):
    return spark.createDataFrame(
        [
            {"Staff Number": "S1", "First Name": "Carol", "Email Address": "carol@example.com"},
            {"Staff Number": "S2", "First Name": "Dave", "Email Address": "dave@example.com"},
        ]
    )


def _anon_horizon_df(spark):
    return spark.createDataFrame(
        [
            {"Staff Number": "S1", "First Name": "REDACTED", "Email Address": "hash_carol"},
            {"Staff Number": "S2", "First Name": "REDACTED", "Email Address": "hash_dave"},
        ]
    )


# ---------------------------------------------------------------------------
# Factory — recreates the notebook function under test in a controlled scope
# ---------------------------------------------------------------------------


def _make_sb_apply_fn(spark, storage_account, log_fn):
    """Return apply_anonymisation_sb as it appears in py_sb_raw_to_std, but with
    notebook globals supplied as arguments so it can be invoked from tests."""

    def apply_anonymisation_sb(df, entity_name):
        anon_enabled = Util.is_non_production_environment()
        log_fn(f"anonymisation_gate: environment={Util.get_environment()} enabled={anon_enabled} entity={entity_name}")
        if not anon_enabled:
            return df
        anon_config = AnonymisationConfig()
        try:
            policy_path = f"abfss://odw-config@{storage_account}anonymisation/policy.yaml"
            policy_text = spark.read.text(policy_path, wholetext=True).first().value
            anon_config = load_config(text=policy_text)
        except Exception as config_err:
            log_fn(f"Could not load anonymisation policy, using defaults: {config_err}")
        engine = AnonymisationEngine(
            config=AnonymisationConfig(
                classification_allowlist=anon_config.classification_allowlist,
                seed_column=anon_config.get_seed_column(entity_name),
            )
        )
        log_fn(f"Applying anonymisation to Service Bus entity: {entity_name}")
        return engine.apply_from_purview(df, entity_name=entity_name, source_folder="ServiceBus")

    return apply_anonymisation_sb


def _make_horizon_anon_block(spark, storage_account, source_folder, log_fn, log_error_fn):
    """Return the anonymisation logic from ingest_adhoc in py_horizon_raw_to_std,
    extracted as a callable for test purposes."""

    def apply_anonymisation_horizon(sparkDF, filename, source_filename_start):
        _anon_enabled = Util.is_non_production_environment()
        log_fn(f"anonymisation_gate: environment={Util.get_environment()} enabled={_anon_enabled} file={filename}")
        if not _anon_enabled:
            return sparkDF
        _anon_config = AnonymisationConfig()
        try:
            policy_path = f"abfss://odw-config@{storage_account}anonymisation/policy.yaml"
            policy_text = spark.read.text(policy_path, wholetext=True).first().value
            _anon_config = load_config(text=policy_text)
        except Exception as config_err:
            log_fn(f"Could not load anonymisation policy, using defaults: {config_err}")
        engine = AnonymisationEngine(
            config=AnonymisationConfig(
                classification_allowlist=_anon_config.classification_allowlist,
                seed_column=_anon_config.get_seed_column(source_filename_start),
            )
        )
        log_fn(f"Applying anonymisation to Horizon file: {filename}")
        try:
            sparkDF = engine.apply_from_purview(sparkDF, file_name=filename, source_folder=source_folder)
        except Exception as anon_err:
            log_error_fn(f"Anonymisation failed for {filename}: {str(anon_err)}")
            raise
        return sparkDF

    return apply_anonymisation_horizon


# ---------------------------------------------------------------------------
# py_sb_raw_to_std tests
# ---------------------------------------------------------------------------


class TestSBNotebookAnonymisation(SparkTestCase):
    def test__applies_anonymisation_in_dev(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_sb_df(spark)
        anon_df = _anon_sb_df(spark)
        log = mock.Mock()

        fn = _make_sb_apply_fn(spark, "test.dfs.core.windows.net/", log)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview", return_value=anon_df) as mock_apply,
        ):
            result = fn(raw_df, "service-user")

        mock_apply.assert_called_once()
        _, kwargs = mock_apply.call_args
        assert kwargs["entity_name"] == "service-user"
        assert kwargs["source_folder"] == "ServiceBus"
        assert result is anon_df

    def test__skips_anonymisation_in_prod(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_sb_df(spark)
        log = mock.Mock()

        fn = _make_sb_apply_fn(spark, "test.dfs.core.windows.net/", log)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=False),
            mock.patch.object(Util, "get_environment", return_value="PROD"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview") as mock_apply,
        ):
            result = fn(raw_df, "service-user")

        mock_apply.assert_not_called()
        assert result is raw_df

    def test__logs_gate_in_both_environments(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_sb_df(spark)
        log = mock.Mock()

        fn = _make_sb_apply_fn(spark, "test.dfs.core.windows.net/", log)

        for env, enabled in [("DEV", True), ("PROD", False)]:
            log.reset_mock()
            with (
                mock.patch.object(Util, "is_non_production_environment", return_value=enabled),
                mock.patch.object(Util, "get_environment", return_value=env),
                mock.patch.object(AnonymisationEngine, "apply_from_purview", return_value=raw_df),
            ):
                fn(raw_df, "appeal-has")

            first_call_msg = log.call_args_list[0][0][0]
            assert f"environment={env}" in first_call_msg
            assert f"enabled={enabled}" in first_call_msg
            assert "entity=appeal-has" in first_call_msg

    def test__raises_when_engine_fails(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_sb_df(spark)
        log = mock.Mock()

        fn = _make_sb_apply_fn(spark, "test.dfs.core.windows.net/", log)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=RuntimeError("Purview failed"),
            ),
        ):
            try:
                fn(raw_df, "service-user")
                assert False, "Expected RuntimeError to propagate"
            except RuntimeError as exc:
                assert str(exc) == "Purview failed"

    def test__uses_default_config_when_policy_load_fails(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_sb_df(spark)
        anon_df = _anon_sb_df(spark)
        log = mock.Mock()

        fn = _make_sb_apply_fn(spark, "test.dfs.core.windows.net/", log)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview", return_value=anon_df),
        ):
            # policy load will fail (no real ADLS) — function should continue with defaults
            result = fn(raw_df, "service-user")

        assert result is anon_df
        fallback_logged = any("Could not load anonymisation policy" in str(call) for call in log.call_args_list)
        assert fallback_logged


# ---------------------------------------------------------------------------
# py_horizon_raw_to_std tests
# ---------------------------------------------------------------------------


class TestHorizonNotebookAnonymisation(SparkTestCase):
    def test__applies_anonymisation_in_dev(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_horizon_df(spark)
        anon_df = _anon_horizon_df(spark)
        log = mock.Mock()
        log_err = mock.Mock()

        fn = _make_horizon_anon_block(spark, "test.dfs.core.windows.net/", "Horizon", log, log_err)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview", return_value=anon_df) as mock_apply,
        ):
            result = fn(raw_df, "Employees.csv", "Employees")

        mock_apply.assert_called_once()
        _, kwargs = mock_apply.call_args
        assert kwargs["file_name"] == "Employees.csv"
        assert kwargs["source_folder"] == "Horizon"
        assert result is anon_df

    def test__skips_anonymisation_in_prod(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_horizon_df(spark)
        log = mock.Mock()

        fn = _make_horizon_anon_block(spark, "test.dfs.core.windows.net/", "Horizon", log, mock.Mock())

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=False),
            mock.patch.object(Util, "get_environment", return_value="PROD"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview") as mock_apply,
        ):
            result = fn(raw_df, "Employees.csv", "Employees")

        mock_apply.assert_not_called()
        assert result is raw_df

    def test__logs_gate_with_filename(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_horizon_df(spark)
        log = mock.Mock()

        fn = _make_horizon_anon_block(spark, "test.dfs.core.windows.net/", "Horizon", log, mock.Mock())

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview", return_value=raw_df),
        ):
            fn(raw_df, "Addresses.csv", "Addresses")

        gate_msg = log.call_args_list[0][0][0]
        assert "environment=DEV" in gate_msg
        assert "enabled=True" in gate_msg
        assert "file=Addresses.csv" in gate_msg

    def test__raises_and_logs_error_when_engine_fails(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        raw_df = _raw_horizon_df(spark)
        log = mock.Mock()
        log_err = mock.Mock()

        fn = _make_horizon_anon_block(spark, "test.dfs.core.windows.net/", "Horizon", log, log_err)

        with (
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=RuntimeError("engine error"),
            ),
        ):
            try:
                fn(raw_df, "Employees.csv", "Employees")
                assert False, "Expected RuntimeError"
            except RuntimeError as exc:
                assert str(exc) == "engine error"

        logged_error = log_err.call_args_list[0][0][0]
        assert "Anonymisation failed" in logged_error
        assert "Employees.csv" in logged_error
