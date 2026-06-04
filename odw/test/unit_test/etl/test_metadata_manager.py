from odw.core.etl.metadata_manager import MetadataManager
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLFailResult
from odw.core.io.synapse_delta_io import SynapseDeltaIO
from odw.core.util.util import Util
from odw.test.util.test_case import SparkTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.util import format_to_adls_path, generate_local_path
from odw.test.util.assertion import assert_dataframes_equal
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from delta.exceptions import ConcurrentAppendException
from datetime import datetime, timedelta
import mock
import pytest
import json


class TestMetadataManager(SparkTestCase):
    MOCK_METADATA_TABLE = "test_unit_odw_execution_history"
    TEST_START_TIME = datetime.now()
    TEST_END_TIME = datetime.now() + timedelta(1)

    @pytest.fixture(scope="module", autouse=True)
    def setup(self, request):
        spark = PytestSparkSessionUtil().get_spark_session()
        DeltaTable.createIfNotExists(spark).tableName(f"odw_meta_db.{self.MOCK_METADATA_TABLE}").addColumns(
            StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"])
        ).location(self.MOCK_METADATA_TABLE).execute()
        with (
            mock.patch.object(MetadataManager, "METADATA_TABLE", self.MOCK_METADATA_TABLE),
            mock.patch.object(SynapseDeltaIO, "_format_to_adls_path", format_to_adls_path),
            mock.patch.object(Util, "get_storage_account", return_value="pinsstodwdevuks9h80mb.dfs.core.windows.net"),
            mock.patch.object(Util, "get_path_to_file", generate_local_path),
        ):
            yield

    def test__metadata_manager__create(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_c"
        entity_name = "some_entity"
        stage_name = "some_stage"
        inst = MetadataManager(spark, run_id, entity_name, stage_name, {"some": "parameters"})
        inst.create()
        relevant_entries = spark.sql(
            (
                f"select * from odw_meta_db.{self.MOCK_METADATA_TABLE} where run_id = '{run_id}' and entity_name = '{entity_name}' and stage_name = '{stage_name}' "
            )
        )
        assert relevant_entries.count() == 1

    def test__metadata_manager__create__fails_for_retry_for_same_entry(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_c_ffrfse"
        entity_name = "some_entity"
        stage_name = "some_stage"
        inst = MetadataManager(spark, run_id, entity_name, stage_name, {"some": "parameters"})
        inst.create()
        with pytest.raises(RuntimeError):
            inst.create()

    def test__metadata_manager__create__fails_with_missing_parameters(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_c_fwmp"
        inst = MetadataManager(spark, run_id)
        with pytest.raises(ValueError):
            inst.create()

    def test__metadata_manager__create__create_multiple_different_entries(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id_prefix = "tu_mm_c_cmde_"
        entries_to_create = (
            (f"{run_id_prefix}a", "entity_a", "stage_a"),
            (f"{run_id_prefix}a", "entity_a", "stage_b"),
            (f"{run_id_prefix}a", "entity_b", "stage_a"),
            (f"{run_id_prefix}a", "entity_b", "stage_b"),
            (f"{run_id_prefix}b", "entity_a", "stage_a"),
            (f"{run_id_prefix}b", "entity_a", "stage_b"),
            (f"{run_id_prefix}b", "entity_b", "stage_a"),
            (f"{run_id_prefix}b", "entity_b", "stage_b"),
        )
        for entry in entries_to_create:
            inst = MetadataManager(spark, entry[0], entry[1], entry[2])
            inst.create()
        relevant_entries = spark.sql(f"select * from odw_meta_db.{self.MOCK_METADATA_TABLE} where run_id like '{run_id_prefix}%'")
        assert relevant_entries.count() == 8

    def assert_update(self, etl_result: ETLResult, expected_outcome: bool):
        id_postfix = "success" if expected_outcome else "fail"
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = f"tu_mm_u_{id_postfix}"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                }
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id, entity_name, stage_name)
        inst._created_entry = True
        inst.update(etl_result)
        relevant_entries = spark.sql(
            (
                f"select * from odw_meta_db.{self.MOCK_METADATA_TABLE} where run_id = '{run_id}' and entity_name = '{entity_name}' and stage_name = '{stage_name}' "
            )
        )
        expected_data = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": self.TEST_END_TIME,
                    "successful": expected_outcome,
                    "result_text": etl_result.model_dump_json(indent=4),
                }
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        assert_dataframes_equal(expected_data, relevant_entries)

    def test__metadata_manager__update__successful_etl_result(self):
        self.assert_update(
            ETLSuccessResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=self.TEST_START_TIME,
                    end_execution_time=self.TEST_END_TIME,
                    table_name="some_table",
                    insert_count=1,
                    update_count=2,
                    delete_count=3,
                    activity_type="some activity",
                    duration_seconds=(self.TEST_END_TIME - self.TEST_START_TIME).total_seconds(),
                )
            ),
            True,
        )

    def test__metadata_manager__update__failed_etl_result(self):
        self.assert_update(
            ETLFailResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=self.TEST_START_TIME,
                    end_execution_time=self.TEST_END_TIME,
                    table_name="some_table",
                    insert_count=0,
                    update_count=0,
                    delete_count=0,
                    activity_type="some activity",
                    duration_seconds=(self.TEST_END_TIME - self.TEST_START_TIME).total_seconds(),
                )
            ),
            False,
        )

    def test__metadata_manager__update__fails_if_entry_not_created_first(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_u_fiencf"
        entity_name = "some_entity"
        stage_name = "some_stage"
        inst = MetadataManager(spark, run_id, entity_name, stage_name)
        inst._created_entry = False
        with pytest.raises(RuntimeError):
            inst.update(
                ETLSuccessResult(
                    metadata=ETLResult.ETLResultMetadata(
                        start_execution_time=self.TEST_START_TIME,
                        end_execution_time=self.TEST_END_TIME,
                        table_name="some_table",
                        insert_count=1,
                        update_count=2,
                        delete_count=3,
                        activity_type="some activity",
                        duration_seconds=(self.TEST_END_TIME - self.TEST_START_TIME).total_seconds(),
                    )
                )
            )

    def test__metadata_manager__update__fails_with_missing_parameters(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_u_fwmp"
        inst = MetadataManager(spark, run_id)
        inst._created_entry = True
        with pytest.raises(ValueError):
            inst.update(
                ETLSuccessResult(
                    metadata=ETLResult.ETLResultMetadata(
                        start_execution_time=self.TEST_START_TIME,
                        end_execution_time=self.TEST_END_TIME,
                        table_name="some_table",
                        insert_count=1,
                        update_count=2,
                        delete_count=3,
                        activity_type="some activity",
                        duration_seconds=(self.TEST_END_TIME - self.TEST_START_TIME).total_seconds(),
                    )
                )
            )

    def test__metadata_manager__get_for_run_id(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_gfri"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": f"{run_id}_some_other_id",
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        expected_result = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id)
        result = inst.get_for_run_id()
        assert_dataframes_equal(expected_result, result)

    def test__metadata_manager__get_for_entity(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_gfe"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": "some other entity",
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        expected_result = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id, entity_name)
        result = inst.get_for_entity()
        assert_dataframes_equal(expected_result, result)

    def test__metadata_manager__get_for_entity__fails_with_missing_parameters(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_gfe_wmp"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": "some other entity",
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id)
        with pytest.raises(RuntimeError):
            inst.get_for_entity()

    def test__metadata_manager__get_for_entity_stage(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_gfes"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": "some_other_stage",
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": f"{run_id}_some_other_id",
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": "some other entity",
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        expected_result = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id, entity_name, stage_name)
        result = inst.get_for_entity_stage()
        assert_dataframes_equal(expected_result, result)

    def test__metadata_manager__get_for_entity_stage__fails_with_missing_parameters(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_gfes_wmp"
        entity_name = "some_entity"
        stage_name = "some_stage"
        execution_parameters = {"some": "parameters"}
        existing_entry: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": "some_other_stage",
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": f"{run_id}_some_other_id",
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
                {
                    "run_id": run_id,
                    "entity_name": "some other entity",
                    "stage_name": stage_name,
                    "execution_parameters": json.dumps(execution_parameters, default=str),
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        existing_entry.write.format("delta").mode("append").insertInto(f"odw_meta_db.{self.MOCK_METADATA_TABLE}")
        inst = MetadataManager(spark, run_id, entity_name)
        with pytest.raises(RuntimeError):
            inst.get_for_entity_stage()

    def test__metadata_manager__write__retries_on_failure(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_w_rof"
        entity_name = "some_entity"
        stage_name = "some_stage"
        entry_to_write: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": {"some": "parameters"},
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        side_effects = [ConcurrentAppendException(desc="some exception", stackTrace="some trace"), None]
        with mock.patch.object(SynapseDeltaIO, "write", side_effect=side_effects):
            inst = MetadataManager(spark, run_id, entity_name, stage_name)
            inst._write(entry_to_write)
            assert SynapseDeltaIO.write.call_count == 2

    def test__metadata_manager__write__only_retries_on_concurrent_append_exception(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_w_orocae"
        entity_name = "some_entity"
        stage_name = "some_stage"
        entry_to_write: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": {"some": "parameters"},
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        side_effects = [
            ValueError("some exception"),
        ]
        with mock.patch.object(SynapseDeltaIO, "write", side_effect=side_effects):
            with pytest.raises(ValueError):
                inst = MetadataManager(spark, run_id, entity_name, stage_name)
                inst._write(entry_to_write)

    def test__metadata_manager__write__max_retries(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        run_id = "tu_mm_w_mr"
        entity_name = "some_entity"
        stage_name = "some_stage"
        entry_to_write: DataFrame = spark.createDataFrame(
            [
                {
                    "run_id": run_id,
                    "entity_name": entity_name,
                    "stage_name": stage_name,
                    "execution_parameters": {"some": "parameters"},
                    "execution_start_time": self.TEST_START_TIME,
                    "execution_finish_time": None,
                    "successful": None,
                    "result_text": None,
                },
            ],
            schema=StructType([field for field in MetadataManager.METADATA_SCHEMA.fields if field.name != "_update_key_col"]),
        )
        side_effects = [
            ConcurrentAppendException(desc="some exception", stackTrace="some trace"),
        ] * 10
        with (
            mock.patch.object(SynapseDeltaIO, "write", side_effect=side_effects),
        ):
            with pytest.raises(ConcurrentAppendException):
                inst = MetadataManager(spark, run_id, entity_name, stage_name)
                inst._write.retry.wait.min = 0
                inst._write.retry.wait.max = 0.1
                inst._write(entry_to_write)
