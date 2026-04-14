import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.appeal_representation_curation_process import AppealRepresentationCurationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from datetime import datetime
import pyspark.sql.types as T
import mock


pytestmark = pytest.mark.xfail(reason="Curated logic not implemented yet")


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
            None,
            None,
            ["bbbbb"],
            1,
            "ODT",
            5,
            datetime(2025, 1, 2, 0, 0, 0, 0).isoformat(),
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
            None,
            None,
            ["ddddd"],
            1,
            "ODT",
            5,
            datetime(2025, 1, 4, 0, 0, 0, 0).isoformat(),
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
                    1,
                    "ODT",
                    5,
                    datetime(2025, 1, 3, 0, 0, 0, 0).isoformat(),
                    None,
                    None,
                    "N",  # This entry should be dropped by the curation process
                    "ccccc",
                )
            ],
            schema=T.StructType(
                [
                    T.StructField("appealRepresentaionID", T.LongType(), True),
                    T.StructField("representationId", T.StringType(), True),
                    T.StructField("caseId", T.LongType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("representationStatus", T.StringType(), True),
                    T.StructField("originalRepresentation", T.StringType(), True),
                    T.StructField("redacted", T.BooleanType(), True),
                    T.StructField("redactedRepresentation", T.StringType(), True),
                    T.StructField("redactedBy", T.StringType(), True),
                    T.StructField("invalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                    T.StructField("otherInvalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                    T.StructField("source", T.StringType(), True),
                    T.StructField("serviceUserId", T.StringType(), True),
                    T.StructField("representationType", T.StringType(), True),
                    T.StructField("dateReceived", T.StringType(), True),
                    T.StructField("documentIds", T.ArrayType(T.StringType(), True), True),
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
                T.StructField("invalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
                T.StructField("otherInvalidOrIncompleteDetails", T.ArrayType(T.StringType(), True), True),
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
                    2,
                    3,
                    603,
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
                    3,
                    4,
                    604,
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
                    6,
                    7,
                    607,
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
        - Given I a have an existing appeal representation table and some new appeal representation data in the harmonised layer
        - When I run the appeal representation curation process
        - Then the new data should be overwritten
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_harmonised_data = self.generate_harmonised_table()
        self.write_existing_table(
            spark,
            existing_harmonised_data,
            "sb_test_arcp_r_ed",
            "odw_harmonised_db",
            "odw-harmonised",
            "sb_appeal_representation",
            "overwrite",
        )
        # Existing data should be overwritten
        existing_curated_data = spark.createDataFrame(
            [
                (
                    10,
                    11,
                    12,
                    612,
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
                    1,
                    "ODT",
                    5,
                    datetime(2025, 1, 1, 0, 0, 0, 0).isoformat(),
                    None,
                    None,
                    "Y",
                    "xxxxx",
                ),
                (
                    11,
                    12,
                    13,
                    613,
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
                    1,
                    "ODT",
                    5,
                    datetime(2026, 1, 1, 0, 0, 0, 0).isoformat(),
                    None,
                    None,
                    "Y",
                    "yyyyy",
                ),
            ],
            self.generate_curated_data_schema(),
        )
        # Create curated table
        self.write_existing_table(
            spark,
            existing_curated_data,
            "test_arcp_r_ed",
            "odw_curated_db",
            "odw-curated",
            "appeal_representation",
            "overwrite",
        )
        expected_curated_data_after_writing = self.generate_curated_data()
        with mock.patch.object(AppealRepresentationCurationProcess, "HARMONISED_TABLE", "sb_test_arcp_r_ed"):
            with mock.patch.object(AppealRepresentationCurationProcess, "CURATED_TABLE", "test_arcp_r_ed"):
                inst = AppealRepresentationCurationProcess(spark)
                result = inst.run()
                assert_etl_result_successful(result)
                actual_table_data = spark.table("odw_curated_db.test_arcp_r_ed")
                assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)

    def test__appeal_representation_curation_process__run__with_no_existing_data(self):
        """
        - Given I have some new appeal representation data in the harmonised layer, and this is the first time the data is being ingested
        - When I run the appeal representation curation process
        - Then the the appeal representation table should be created and the data added to it
        """
        spark = PytestSparkSessionUtil().get_spark_session()
        existing_harmonised_data = self.generate_harmonised_table()
        self.write_existing_table(
            spark,
            existing_harmonised_data,
            "sb_test_arcp_r_ned",
            "odw_harmonised_db",
            "odw-harmonised",
            "sb_appeal_representation",
            "overwrite",
        )
        expected_curated_data_after_writing = self.generate_curated_data()
        with mock.patch.object(AppealRepresentationCurationProcess, "HARMONISED_TABLE", "sb_test_arcp_r_ned"):
            with mock.patch.object(AppealRepresentationCurationProcess, "CURATED_TABLE", "test_arcp_r_ned"):
                inst = AppealRepresentationCurationProcess(spark)
                result = inst.run()
                assert_etl_result_successful(result)
                actual_table_data = spark.table("odw_curated_db.test_arcp_r_ned")
                assert_dataframes_equal(expected_curated_data_after_writing, actual_table_data)
