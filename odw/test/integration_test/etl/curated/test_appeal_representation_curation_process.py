import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_representation_curation_process import (
    AppealRepresentationCurationProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import (
    assert_dataframes_equal,
    assert_etl_result_successful,
)
from datetime import datetime
import pyspark.sql.types as T
import mock


class TestAppealRepresentationCurationProcess(ETLTestCase):
    ACTIVE_HARMONISED_DATA = [
        (
            1,
            "2",
            3,
            "603",
            "published",
            "Some description",
            False,
            None,
            None,
            [],
            [],
            "lpa",
            None,
            "statement",
            datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
            ["aaaaa"],
            1,
            "ODT",
            5,
            datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
            None,
            None,
            "Y",
            "aaaaa",
        ),
        (
            2,
            "3",
            4,
            "604",
            "published",
            "Another description",
            False,
            None,
            None,
            [],
            [],
            "lpa",
            None,
            "statement",
            datetime(2025, 1, 2, 0, 0, 0, 0).isoformat(),
            ["bbbbb"],
            "1",
            "ODT",
            "5",
            datetime(2025, 1, 2, 0, 0, 0, 0).isoformat(),
            None,
            None,
            "Y",
            "bbbbb",
        ),
        (
            5,
            "6",
            7,
            "607",
            "published",
            "Another description 3",
            False,
            None,
            None,
            [],
            [],
            "lpa",
            None,
            "statement",
            datetime(2025, 1, 4, 0, 0, 0, 0).isoformat(),
            ["ddddd"],
            1,
            "ODT",
            5,
            datetime(2025, 1, 4, 0, 0, 0, 0).isoformat(),
            None,
            None,
            "Y",
            "ddddd",
        ),
    ]

    def generate_harmonised_table(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        return spark.createDataFrame(
            data=self.ACTIVE_HARMONISED_DATA
            + [
                (
                    3,
                    "4",
                    5,
                    "605",
                    "published",
                    "Another description 2",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2025, 1, 3, 0, 0, 0, 0).isoformat(),
                    ["ccccc"],
                    "1",
                    "ODT",
                    "5",
                    datetime(2025, 1, 3, 0, 0, 0, 0).isoformat(),
                    None,
                    None,
                    "N",  # This entry should be dropped by the curation process
                    "ccccc",
                )
            ],
            schema=T.StructType(
                [
                    T.StructField("appealRepresentationID", T.LongType(), True),
                    T.StructField("representationId", T.StringType(), True),
                    T.StructField("caseId", T.LongType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("representationStatus", T.StringType(), True),
                    T.StructField("originalRepresentation", T.StringType(), True),
                    T.StructField("redacted", T.BooleanType(), True),
                    T.StructField("redactedRepresentation", T.StringType(), True),
                    T.StructField("redactedBy", T.StringType(), True),
                    T.StructField(
                        "invalidOrIncompleteDetails",
                        T.ArrayType(T.StringType(), True),
                        True,
                    ),
                    T.StructField(
                        "otherInvalidOrIncompleteDetails",
                        T.ArrayType(T.StringType(), True),
                        True,
                    ),
                    T.StructField("source", T.StringType(), True),
                    T.StructField("serviceUserId", T.StringType(), True),
                    T.StructField("representationType", T.StringType(), True),
                    T.StructField("dateReceived", T.StringType(), True),
                    T.StructField(
                        "documentIds", T.ArrayType(T.StringType(), True), True
                    ),
                    T.StructField("migrated", T.StringType(), True),
                    T.StructField("ODTSourceSystem", T.StringType(), True),
                    T.StructField("SourceSystemID", T.StringType(), True),
                    T.StructField("IngestionDate", T.StringType(), True),
                    T.StructField("ValidTo", T.StringType(), True),
                    T.StructField("RowID", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                    T.StructField("message_id", T.StringType(), True),
                ]
            ),
        )

    def generate_curated_data_schema(self):
        return T.StructType(
            [
                T.StructField("representationId", T.StringType(), True),
                T.StructField("caseId", T.LongType(), True),
                T.StructField("caseReference", T.StringType(), True),
                T.StructField("representationStatus", T.StringType(), True),
                T.StructField("originalRepresentation", T.StringType(), True),
                T.StructField("redacted", T.BooleanType(), True),
                T.StructField("redactedRepresentation", T.StringType(), True),
                T.StructField("redactedBy", T.StringType(), True),
                T.StructField(
                    "invalidOrIncompleteDetails",
                    T.ArrayType(T.StringType(), True),
                    True,
                ),
                T.StructField(
                    "otherInvalidOrIncompleteDetails",
                    T.ArrayType(T.StringType(), True),
                    True,
                ),
                T.StructField("source", T.StringType(), True),
                T.StructField("serviceUserId", T.StringType(), True),
                T.StructField("representationType", T.StringType(), True),
                T.StructField("dateReceived", T.StringType(), True),
                T.StructField("documentIds", T.ArrayType(T.StringType(), True), True),
            ]
        )

    def generate_curated_data(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        return spark.createDataFrame(
            data=(
                (
                    "2",
                    3,
                    "603",
                    "published",
                    "Some description",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
                    ["aaaaa"],
                ),
                (
                    "3",
                    4,
                    "604",
                    "published",
                    "Another description",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2025, 1, 2, 0, 0, 0, 0).isoformat(),
                    ["bbbbb"],
                ),
                (
                    "6",
                    7,
                    "607",
                    "published",
                    "Another description 3",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2025, 1, 4, 0, 0, 0, 0).isoformat(),
                    ["ddddd"],
                ),
            ),
            schema=self.generate_curated_data_schema(),
        )

    def test__appeal_representation_curation_process__run__with_existing_data(self):
        """
        - Given an existing curated appeal_representation table and new harmonised
          appeal_representation data
        - When the curation process runs
        - Then the curated table should be overwritten with the active harmonised rows
        """
        spark = PytestSparkSessionUtil().get_spark_session()

        # Write the harmonised fixture (read by the class via HARMONISED_TABLE)
        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            "sb_test_arcp_r_ed",
            "odw_harmonised_db",
            "odw-harmonised",
            "sb_appeal_representation",
            "overwrite",
        )

        # Pre-seed the real curated target so we can verify it gets overwritten.
        # The class writes to OUTPUT_TABLE = "odw_curated_db.appeal_representation".
        existing_curated_data = spark.createDataFrame(
            [
                (
                    "11",
                    12,
                    "612",
                    "published",
                    "Some description",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
                    ["xxxxx"],
                ),
                (
                    "12",
                    13,
                    "613",
                    "published",
                    "Some description",
                    False,
                    None,
                    None,
                    [],
                    [],
                    "lpa",
                    None,
                    "statement",
                    datetime(2026, 1, 1, 0, 0, 0, 0).isoformat(),
                    ["yyyyy"],
                ),
            ],
            self.generate_curated_data_schema(),
        )
        self.write_existing_table(
            spark,
            existing_curated_data,
            "appeal_representation",
            "odw_curated_db",
            "odw-curated",
            "appeal_representation",
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        with mock.patch.object(
            AppealRepresentationCurationProcess,
            "HARMONISED_TABLE",
            "odw_harmonised_db.sb_test_arcp_r_ed",
        ):
            inst = AppealRepresentationCurationProcess(spark)
            result = inst.run(
                orchestration_run_id="t_arcp_r_wed",
                orchestration_entity_name="appeal_representation",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)
            actual_table_data = spark.table("odw_curated_db.appeal_representation")
            assert_dataframes_equal(
                expected_curated_data_after_writing, actual_table_data
            )

    def test__appeal_representation_curation_process__run__with_no_existing_data(self):
        """
        - Given harmonised appeal_representation data and no existing curated table
        - When the curation process runs for the first time
        - Then the curated table should be created with the active harmonised rows
        """
        spark = PytestSparkSessionUtil().get_spark_session()

        # Drop any curated table left behind by other tests to simulate first load
        spark.sql("DROP TABLE IF EXISTS odw_curated_db.appeal_representation")

        self.write_existing_table(
            spark,
            self.generate_harmonised_table(),
            "sb_test_arcp_r_ned",
            "odw_harmonised_db",
            "odw-harmonised",
            "sb_appeal_representation",
            "overwrite",
        )

        expected_curated_data_after_writing = self.generate_curated_data()

        with mock.patch.object(
            AppealRepresentationCurationProcess,
            "HARMONISED_TABLE",
            "odw_harmonised_db.sb_test_arcp_r_ned",
        ):
            inst = AppealRepresentationCurationProcess(spark)
            result = inst.run(
                orchestration_run_id="t_arcp_r_wned",
                orchestration_entity_name="appeal_representation",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)
            actual_table_data = spark.table("odw_curated_db.appeal_representation")
            assert_dataframes_equal(
                expected_curated_data_after_writing, actual_table_data
            )
