import json
import mock
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
import pyspark.sql.types as T
from odw.core.etl.transformation.harmonised.horizon_harmonisation_process import HorizonHarmonisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil


_SOURCE_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType(), True),
        T.StructField("name", T.StringType(), True),
        T.StructField("expected_from", T.StringType(), True),
    ]
)


def _orchestration_df(spark, source_system_id="HORIZON-TEST"):
    """Build a single-row orchestration DataFrame matching what load_orchestration_data() returns."""
    definition = {
        "Source_Filename_Start": "test_entity",
        "Standardised_Table_Name": "test_entity",
        "Harmonised_Table_Name": "test_entity",
        "Entity_Primary_Key": "id",
    }
    if source_system_id is not None:
        definition["Source_System_ID"] = source_system_id
    payload = {"definitions": [definition]}
    return spark.read.json(spark.sparkContext.parallelize([json.dumps(payload)]))


class TestHorizonHarmonisationProcessIntegration(ETLTestCase):
    OUTPUT_TABLE = f"{HorizonHarmonisationProcess.HRM_DB}.test_entity"

    def _run(self, spark, source_df, source_system_id="HORIZON-TEST", standardised_table_schema=None):
        source_data = {
            "orchestration_data": _orchestration_df(spark, source_system_id=source_system_id),
            "source_data": source_df,
            "standardised_table_schema": standardised_table_schema,
        }

        with (
            mock.patch(
                "odw.core.etl.transformation.harmonised.horizon_harmonisation_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_etl_logging,
            mock.patch("odw.core.etl.transformation.harmonised.horizon_harmonisation_process.LoggingUtil") as mock_proc_logging,
        ):
            mock_etl_logging.return_value = mock.Mock()
            mock_proc_logging.return_value = mock.Mock()

            inst = HorizonHarmonisationProcess(spark)

            with (
                mock.patch.object(inst, "load_data", return_value=source_data),
                mock.patch.object(inst, "write_data") as mock_write,
            ):
                result = inst.run(entity_name="test_entity")

        return inst, mock_write.call_args[0][0], result

    def test__horizon_harmonisation_process__run__initial_load_writes_single_active_row_with_plumbing(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [("id-1", "Alice", "2024-01-01 00:00:00")],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, result = self._run(spark, source_df)

        write_config = data_to_write[self.OUTPUT_TABLE]
        df = write_config["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["id"] == "id-1"
        assert rows[0]["name"] == "Alice"
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert rows[0]["ODTSourceSystem"] == "Horizon"
        assert rows[0]["Migrated"] == "0"
        assert rows[0]["SourceSystemID"] == "HORIZON-TEST"
        assert rows[0]["IngestionDate"] is not None
        # RowID must be a populated MD5 hex digest (32 chars), not an empty placeholder.
        assert rows[0]["RowID"] is not None
        assert len(rows[0]["RowID"]) == 32

        assert write_config["write_mode"] == "overwrite"
        assert write_config["partition_by"] == ["IsActive"]
        assert write_config["file_format"] == "delta"
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__horizon_harmonisation_process__run__state_change_for_same_key_produces_scd2_history(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [
                ("id-1", "Alice", "2024-01-01 00:00:00"),
                ("id-1", "Alice Updated", "2024-02-01 00:00:00"),
            ],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, result = self._run(spark, source_df)

        df = data_to_write[self.OUTPUT_TABLE]["data"]
        rows = {row["name"]: row for row in df.collect()}

        assert df.count() == 2
        assert rows["Alice"]["IsActive"] == "N"
        assert rows["Alice"]["ValidTo"] is not None
        assert rows["Alice Updated"]["IsActive"] == "Y"
        assert rows["Alice Updated"]["ValidTo"] is None
        assert rows["Alice"]["ValidTo"] == rows["Alice Updated"]["IngestionDate"]
        # Different business state must produce different RowIDs.
        assert rows["Alice"]["RowID"] != rows["Alice Updated"]["RowID"]
        assert result.metadata.insert_count == 2

    def test__horizon_harmonisation_process__run__unchanged_state_across_ingestions_is_deduplicated(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [
                ("id-1", "Alice", "2024-01-01 00:00:00"),
                ("id-1", "Alice", "2024-02-01 00:00:00"),
            ],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, result = self._run(spark, source_df)

        df = data_to_write[self.OUTPUT_TABLE]["data"]
        rows = df.collect()

        assert len(rows) == 1
        assert rows[0]["IsActive"] == "Y"
        assert rows[0]["ValidTo"] is None
        assert result.metadata.insert_count == 1

    def test__horizon_harmonisation_process__run__rows_with_null_primary_key_are_dropped(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [
                ("id-1", "Alice", "2024-01-01 00:00:00"),
                (None, "Orphan", "2024-01-01 00:00:00"),
            ],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, result = self._run(spark, source_df)

        df = data_to_write[self.OUTPUT_TABLE]["data"]
        ids = [row["id"] for row in df.collect()]

        assert ids == ["id-1"]
        assert result.metadata.insert_count == 1

    def test__horizon_harmonisation_process__run__omits_source_system_id_column_when_definition_has_none(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_df = spark.createDataFrame(
            [("id-1", "Alice", "2024-01-01 00:00:00")],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, _ = self._run(spark, source_df, source_system_id=None)

        df = data_to_write[self.OUTPUT_TABLE]["data"]
        assert "SourceSystemID" not in df.columns

    def test__horizon_harmonisation_process__run__rowid_matches_schema_driven_md5_concat_with_dot_for_nulls(self):
        import hashlib

        spark = PytestSparkSessionUtil().get_spark_session()

        # Schema picks the explicit ordered RowID column list — `name` only, `expected_from` is
        # excluded as standardisation metadata, and `id` is excluded because it's not declared.
        schema = {
            "fields": [
                {"name": "ingested_datetime", "type": "timestamp", "nullable": False},
                {"name": "expected_from", "type": "timestamp", "nullable": False},
                {"name": "expected_to", "type": "timestamp", "nullable": False},
                {"name": "id", "type": "string", "nullable": False},
                {"name": "name", "type": "string", "nullable": True},
            ]
        }

        source_df = spark.createDataFrame(
            [
                ("id-1", "Alice", "2024-01-01 00:00:00"),
                ("id-2", None, "2024-01-01 00:00:00"),
            ],
            _SOURCE_SCHEMA,
        )

        _, data_to_write, _ = self._run(spark, source_df, standardised_table_schema=schema)

        df = data_to_write[self.OUTPUT_TABLE]["data"]
        rows = {row["id"]: row for row in df.collect()}

        # Expected RowID = md5(concat(IFNULL(id, '.'), IFNULL(name, '.'))) — schema order, no
        # separator, '.' for nulls. Mirrors the notebook idiom (e.g. appeals_folder.json).
        assert rows["id-1"]["RowID"] == hashlib.md5(b"id-1Alice").hexdigest()
        assert rows["id-2"]["RowID"] == hashlib.md5(b"id-2.").hexdigest()
