"""
Tests for the watermark MERGE logic in nb_sb_write_manifest_and_watermark.

Key scenario: multiple entity pipelines run concurrently and MERGE into the same
service_bus_watermarks Delta table. The ON clause must include ingest_date so Delta
can prune reads to a single (entity, ingest_date) partition and avoid
ConcurrentAppendException across entities.
"""
import pytest
from pyspark.sql import Row, SparkSession

from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

_TABLE = "odw_meta_db.service_bus_watermarks"

_DDL = f"""
CREATE TABLE IF NOT EXISTS {_TABLE} (
  entity            STRING,
  source            STRING,
  last_ingest_time  TIMESTAMP,
  ingest_date       STRING,
  run_id            STRING,
  records_ingested  BIGINT,
  status            STRING,
  updated_at        TIMESTAMP
)
USING DELTA
PARTITIONED BY (entity, ingest_date)
"""

_MERGE = f"""
MERGE INTO {_TABLE} AS tgt
USING wm_upd AS src
ON  tgt.entity      = src.entity
AND tgt.source      = src.source
AND tgt.ingest_date = src.ingest_date
WHEN MATCHED THEN UPDATE SET
  tgt.last_ingest_time = src.last_ingest_time,
  tgt.run_id           = src.run_id,
  tgt.records_ingested = src.records_ingested,
  tgt.status           = src.status,
  tgt.updated_at       = src.updated_at
WHEN NOT MATCHED THEN INSERT (entity, source, last_ingest_time, ingest_date, run_id, records_ingested, status, updated_at)
VALUES (src.entity, src.source, src.last_ingest_time, src.ingest_date, src.run_id, src.records_ingested, src.status, src.updated_at)
"""


def _upsert(spark: SparkSession, entity: str, ingest_date: str, run_id: str, records: int, status: str) -> None:
    df = spark.createDataFrame(
        [Row(entity=entity, source="service-bus", last_ingest_time="2026-06-03T14:00:00Z", ingest_date=ingest_date, run_id=run_id, records_ingested=records, status=status, updated_at="2026-06-03 15:00:00")]
    ).selectExpr(
        "entity",
        "source",
        "to_timestamp(last_ingest_time) as last_ingest_time",
        "ingest_date",
        "run_id",
        "cast(records_ingested as bigint) as records_ingested",
        "status",
        "to_timestamp(updated_at) as updated_at",
    )
    df.createOrReplaceTempView("wm_upd")
    spark.sql(_MERGE)


class TestWatermarkUpsert(SparkTestCase):
    @pytest.fixture(scope="class", autouse=True)
    def setup_meta_db(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        spark.sql("CREATE DATABASE IF NOT EXISTS odw_meta_db")
        spark.sql(f"DROP TABLE IF EXISTS {_TABLE}")
        spark.sql(_DDL)
        yield
        spark.sql(f"DROP TABLE IF EXISTS {_TABLE}")

    @pytest.fixture(scope="function", autouse=True)
    def truncate_table(self, setup_meta_db):
        spark = PytestSparkSessionUtil().get_spark_session()
        spark.sql(f"DELETE FROM {_TABLE}")
        yield

    def test_insert_new_row(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        _upsert(spark, "nsip-document", "2026-06-03", "run-1", 165, "Succeeded")

        rows = spark.sql(f"SELECT * FROM {_TABLE}").collect()
        assert len(rows) == 1
        assert rows[0]["entity"] == "nsip-document"
        assert rows[0]["records_ingested"] == 165
        assert rows[0]["status"] == "Succeeded"

    def test_update_existing_row(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        _upsert(spark, "nsip-document", "2026-06-03", "run-1", 165, "Succeeded")
        _upsert(spark, "nsip-document", "2026-06-03", "run-2", 200, "Succeeded")

        rows = spark.sql(f"SELECT * FROM {_TABLE}").collect()
        assert len(rows) == 1, "re-run of same entity+date should update, not insert"
        assert rows[0]["run_id"] == "run-2"
        assert rows[0]["records_ingested"] == 200

    def test_different_dates_produce_separate_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        _upsert(spark, "nsip-document", "2026-06-02", "run-a", 100, "Succeeded")
        _upsert(spark, "nsip-document", "2026-06-03", "run-b", 165, "Succeeded")

        rows = spark.sql(f"SELECT * FROM {_TABLE} ORDER BY ingest_date").collect()
        assert len(rows) == 2
        assert rows[0]["ingest_date"] == "2026-06-02"
        assert rows[1]["ingest_date"] == "2026-06-03"

    def test_different_entities_do_not_overwrite_each_other(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        _upsert(spark, "nsip-document", "2026-06-03", "run-doc", 165, "Succeeded")
        _upsert(spark, "nsip-project", "2026-06-03", "run-prj", 42, "Succeeded")

        rows = {r["entity"]: r for r in spark.sql(f"SELECT * FROM {_TABLE}").collect()}
        assert len(rows) == 2
        assert rows["nsip-document"]["records_ingested"] == 165
        assert rows["nsip-project"]["records_ingested"] == 42

    def test_sequential_merges_for_different_entities_all_succeed(self):
        """
        Regression for ConcurrentAppendException (THEODW-3003).
        In production, multiple Synapse pools each submit their own MERGE concurrently.
        True concurrent behaviour cannot be reliably replicated with a shared local SparkSession
        (different JVMs / Delta writers are required), so we verify here that sequential MERGEs
        for distinct entities each succeed and produce independent rows.
        """
        spark = PytestSparkSessionUtil().get_spark_session()

        entities = [f"entity-{i}" for i in range(5)]
        for entity in entities:
            _upsert(spark, entity, "2026-06-03", f"run-{entity}", 10, "Succeeded")

        rows = spark.sql(f"SELECT entity FROM {_TABLE} ORDER BY entity").collect()
        assert [r["entity"] for r in rows] == sorted(entities)
