"""
Integration tests for the anonymisation gate in the standardisation ETL pipeline.

These tests run the complete load → process → write flow for both
ServiceBusStandardisationProcess and HorizonStandardisationProcess, verifying that:
  - When Util.is_non_production_environment() returns True (DEV/TEST), AnonymisationEngine
    is invoked with the correct parameters before data is written.
  - When Util.is_non_production_environment() returns False (PROD), the engine is never
    called and data passes through unmodified.
  - When the engine raises in DEV/TEST the ETL result is a failure and no data is written.

AnonymisationEngine.apply_from_purview is mocked to avoid live Purview HTTP calls.
The tests are integration-level because they exercise the full pipeline wiring, not
isolated units.
"""

from odw.test.util.mock.import_mock_notebook_utils import notebookutils  # noqa: F401

import json
import pytest
import mock
from pathlib import Path

import pyspark.sql.functions as F
import pyspark.sql.types as T

from odw.core.anonymisation.engine import AnonymisationEngine
from odw.core.etl.etl_result import ETLFailResult
from odw.core.etl.transformation.standardised.horizon_standardisation_process import (
    HorizonStandardisationProcess,
)
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import (
    ServiceBusStandardisationProcess,
)
from odw.core.etl.util.schema_util import SchemaUtil
from odw.core.io.synapse_data_io import SynapseDataIO
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.util import (
    add_orchestration_entry,
    format_adls_path_to_local_path,
    format_to_adls_path,
    generate_local_path,
    get_all_files_in_directory,
)


_SB_ENTITY = "test_anon_gate_sb"
_HZ_ENTITY = "test_anon_gate_hz"
_MSG_TIME = "2024-06-01T10:00:00.000000+0000"


def _sb_schema():
    return T.StructType(
        [
            T.StructField("id", T.LongType()),
            T.StructField("full_name", T.StringType()),
            T.StructField("email", T.StringType()),
            T.StructField("message_enqueued_time_utc", T.StringType()),
        ]
    )


def _sb_pre_existing_schema():
    """Minimal schema for the pre-existing standardised table (only input_file is required by load_data)."""
    return T.StructType(
        [
            T.StructField("input_file", T.StringType()),
            T.StructField("message_enqueued_time_utc", T.StringType()),
        ]
    )


def _hz_schema_json():
    return {
        "fields": [
            {"metadata": {}, "name": "col_a", "type": "string", "nullable": True},
            {"metadata": {}, "name": "full_name", "type": "string", "nullable": True},
            {"metadata": {}, "name": "email", "type": "string", "nullable": True},
        ]
    }


class TestAnonymisationGateServiceBus(ETLTestCase):
    """AnonymisationEngine is invoked in DEV and bypassed in PROD within ServiceBusStandardisationProcess."""

    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        with (
            mock.patch(
                "notebookutils.mssparkutils.runtime.context",
                {"pipelinejobid": "some_guid", "isForPipeline": True},
            ),
            mock.patch.object(
                SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path
            ),
            mock.patch.object(
                SynapseTableDataIO, "_format_to_adls_path", format_to_adls_path
            ),
            mock.patch.object(
                ServiceBusStandardisationProcess,
                "get_all_files_in_directory",
                get_all_files_in_directory,
            ),
            mock.patch.object(
                Util,
                "get_storage_account",
                return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net",
            ),
            mock.patch.object(Util, "get_path_to_file", generate_local_path),
            mock.patch.object(LoggingUtil, "__new__"),
            mock.patch.object(LoggingUtil, "log_info", return_value=None),
            mock.patch.object(LoggingUtil, "log_error", return_value=None),
        ):
            yield

    def _write_sb_input_files(self, entity_name, rows):
        warehouse = PytestSparkSessionUtil().get_spark_warehouse_name()
        path = Path(warehouse) / "odw-raw" / "ServiceBus" / entity_name
        path.mkdir(parents=True, exist_ok=True)
        for i, row in enumerate(rows):
            with open(path / f"msg{i}.json", "w") as f:
                json.dump([row], f)

    def _create_empty_sb_table(self, spark, entity):
        """ServiceBusStandardisationProcess.load_data requires the standardised table to exist."""
        empty_df = spark.createDataFrame([], _sb_pre_existing_schema())
        self.write_existing_table(
            spark,
            empty_df,
            f"sb_{entity}",
            "odw_standardised_db",
            "odw-standardised",
            f"sb_{entity}",
            "overwrite",
        )

    def test__anonymisation_engine_called_in_dev(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_SB_ENTITY}_dev"
        rows = [
            {
                "id": 1,
                "full_name": "Alice Smith",
                "email": "alice@example.com",
                "message_enqueued_time_utc": _MSG_TIME,
            },
            {
                "id": 2,
                "full_name": "Bob Jones",
                "email": "bob@example.com",
                "message_enqueued_time_utc": _MSG_TIME,
            },
        ]
        self._create_empty_sb_table(spark, entity)
        self._write_sb_input_files(entity, rows)

        with (
            mock.patch.object(
                SchemaUtil, "get_service_bus_schema", return_value=_sb_schema()
            ),
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=lambda df, **kw: df,
            ) as mock_apply,
        ):
            result = ServiceBusStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_sb_dev",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                entity_name=entity,
            )

        assert_etl_result_successful(result)
        mock_apply.assert_called_once()
        _, kwargs = mock_apply.call_args
        assert kwargs.get("entity_name") == entity
        assert kwargs.get("source_folder") == "ServiceBus"

    def test__anonymisation_engine_not_called_in_prod(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_SB_ENTITY}_prod"
        rows = [
            {
                "id": 1,
                "full_name": "Alice Smith",
                "email": "alice@example.com",
                "message_enqueued_time_utc": _MSG_TIME,
            },
        ]
        self._create_empty_sb_table(spark, entity)
        self._write_sb_input_files(entity, rows)

        with (
            mock.patch.object(
                SchemaUtil, "get_service_bus_schema", return_value=_sb_schema()
            ),
            mock.patch.object(
                Util, "is_non_production_environment", return_value=False
            ),
            mock.patch.object(Util, "get_environment", return_value="PROD"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview") as mock_apply,
        ):
            result = ServiceBusStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_sb_prod",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                entity_name=entity,
            )

        assert_etl_result_successful(result)
        mock_apply.assert_not_called()

    def test__anonymisation_failure_returns_etl_fail_result(self):
        """An engine crash in DEV must produce ETLFailResult — the ETL framework catches all process exceptions."""
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_SB_ENTITY}_fail"
        rows = [
            {
                "id": 1,
                "full_name": "Alice Smith",
                "email": "alice@example.com",
                "message_enqueued_time_utc": _MSG_TIME,
            },
        ]
        self._create_empty_sb_table(spark, entity)
        self._write_sb_input_files(entity, rows)

        with (
            mock.patch.object(
                SchemaUtil, "get_service_bus_schema", return_value=_sb_schema()
            ),
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=RuntimeError("engine failed"),
            ),
        ):
            result = ServiceBusStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_sb_fail",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                entity_name=entity,
            )

        assert isinstance(result, ETLFailResult), (
            "Expected ETLFailResult when anonymisation engine raises"
        )
        assert "engine failed" in result.metadata.exception_trace


class TestAnonymisationGateHorizon(ETLTestCase):
    """AnonymisationEngine is invoked in DEV and bypassed in PROD within HorizonStandardisationProcess."""

    _DATE_FOLDER = "2027-06-08"

    @pytest.fixture(scope="module", autouse=True)
    def initialise_orchestration_file(self):
        for suffix in ("dev", "prod", "fail"):
            add_orchestration_entry(
                {
                    "Source_Filename_Start": f"{_HZ_ENTITY}_{suffix}",
                    "Load_Enable_status": "True",
                    "Standardised_Table_Definition": (
                        f"standardised_table_definitions/{_HZ_ENTITY}_{suffix}/{_HZ_ENTITY}_{suffix}.json"
                    ),
                    "Source_Frequency_Folder": "",
                    "Standardised_Table_Name": f"{_HZ_ENTITY}_{suffix}",
                    "Expected_Within_Weekdays": 1,
                }
            )

    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        with (
            mock.patch(
                "notebookutils.mssparkutils.runtime.context",
                {"pipelinejobid": "some_guid", "isForPipeline": True},
            ),
            mock.patch.object(
                SynapseDataIO, "_format_to_adls_path", format_adls_path_to_local_path
            ),
            mock.patch.object(
                SynapseTableDataIO, "_format_to_adls_path", format_to_adls_path
            ),
            mock.patch.object(
                Util,
                "get_storage_account",
                return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net",
            ),
            mock.patch.object(Util, "get_path_to_file", generate_local_path),
            mock.patch.object(LoggingUtil, "__new__"),
            mock.patch.object(LoggingUtil, "log_info", return_value=None),
            mock.patch.object(LoggingUtil, "log_error", return_value=None),
        ):
            yield

    def _prepare_hz_test_data(self, entity):
        csv_data = (
            ("col_a", "full_name", "email"),
            ("A1", "Alice Smith", "alice@example.com"),
            ("A2", "Bob Jones", "bob@example.com"),
        )
        self.write_csv(
            csv_data, ["odw-raw", entity, self._DATE_FOLDER, f"{entity}.csv"]
        )
        self.write_json(
            _hz_schema_json(),
            ["odw-config", "standardised_table_definitions", entity, f"{entity}.json"],
        )

    def test__anonymisation_engine_called_in_dev(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_HZ_ENTITY}_dev"
        self._prepare_hz_test_data(entity)
        spark.sql(f"DROP TABLE IF EXISTS odw_standardised_db.{entity}")

        with (
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_last_modified_folder",
                return_value=self._DATE_FOLDER,
            ),
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_file_names_in_directory",
                return_value=[f"{entity}.csv"],
            ),
            mock.patch.object(
                F, "input_file_name", return_value=F.lit("some_input_file")
            ),
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=lambda df, **kw: df,
            ) as mock_apply,
        ):
            result = HorizonStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_hz_dev",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                source_folder=entity,
            )

        assert_etl_result_successful(result)
        mock_apply.assert_called_once()
        _, kwargs = mock_apply.call_args
        assert kwargs.get("file_name") == f"{entity}.csv"
        assert kwargs.get("source_folder") == "Horizon"

    def test__anonymisation_engine_not_called_in_prod(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_HZ_ENTITY}_prod"
        self._prepare_hz_test_data(entity)
        spark.sql(f"DROP TABLE IF EXISTS odw_standardised_db.{entity}")

        with (
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_last_modified_folder",
                return_value=self._DATE_FOLDER,
            ),
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_file_names_in_directory",
                return_value=[f"{entity}.csv"],
            ),
            mock.patch.object(
                F, "input_file_name", return_value=F.lit("some_input_file")
            ),
            mock.patch.object(
                Util, "is_non_production_environment", return_value=False
            ),
            mock.patch.object(Util, "get_environment", return_value="PROD"),
            mock.patch.object(AnonymisationEngine, "apply_from_purview") as mock_apply,
        ):
            result = HorizonStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_hz_prod",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                source_folder=entity,
            )

        assert_etl_result_successful(result)
        mock_apply.assert_not_called()

    def test__anonymisation_failure_returns_etl_fail_result(self):
        """An engine crash in DEV must produce ETLFailResult — the ETL framework catches all process exceptions."""
        spark = PytestSparkSessionUtil().get_spark_session()
        entity = f"{_HZ_ENTITY}_fail"
        self._prepare_hz_test_data(entity)
        spark.sql(f"DROP TABLE IF EXISTS odw_standardised_db.{entity}")

        with (
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_last_modified_folder",
                return_value=self._DATE_FOLDER,
            ),
            mock.patch.object(
                HorizonStandardisationProcess,
                "get_file_names_in_directory",
                return_value=[f"{entity}.csv"],
            ),
            mock.patch.object(
                F, "input_file_name", return_value=F.lit("some_input_file")
            ),
            mock.patch.object(Util, "is_non_production_environment", return_value=True),
            mock.patch.object(Util, "get_environment", return_value="DEV"),
            mock.patch.object(
                AnonymisationEngine,
                "apply_from_purview",
                side_effect=RuntimeError("engine failed"),
            ),
        ):
            result = HorizonStandardisationProcess(spark).run(
                orchestration_run_id="t_anon_hz_fail",
                orchestration_entity_name="some_entity",
                orchestration_stage_name="standardise",
                source_folder=entity,
            )

        assert isinstance(result, ETLFailResult), (
            "Expected ETLFailResult when anonymisation engine raises"
        )
        assert "engine failed" in result.metadata.exception_trace
        assert not spark.catalog.tableExists(f"odw_standardised_db.{entity}"), (
            "Table must not be created when anonymisation fails — no partial write"
        )
