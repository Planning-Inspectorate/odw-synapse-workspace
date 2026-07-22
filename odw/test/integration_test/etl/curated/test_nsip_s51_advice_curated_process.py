import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_s51_advice_curated_process import (
    NsipS51AdviceCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
import mock


class TestNSIPS51AdviceCurated(ETLTestCase):
    def test__nsip_s51_advice_curated_process__run__applies_expected_mappings(self):
        test_case = "t_ns51acp_r_aem"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_s51_advice = spark.createDataFrame(
            [
                (
                    1,
                    "ADV-1",
                    "None",
                    "EN010001",
                    "Title 1",
                    None,
                    "Fred",
                    "None",
                    "EMAIL",
                    "2026-01-01",
                    "Question 1",
                    None,
                    "Inspector",
                    "2026-01-02",
                    "Answer 1",
                    None,
                    "Not Checked",
                    "None",
                    ["a1"],
                    "Y",
                ),
                (
                    2,
                    "ADV-2",
                    "123",
                    "EN010002",
                    "Title 2",
                    "Teitl 2",
                    "Jane",
                    "Agent Ltd",
                    "None",
                    "2026-02-01",
                    "Question 2",
                    "Cwestiwn 2",
                    "Case Team",
                    "2026-02-02",
                    "Answer 2",
                    "Ateb 2",
                    "Do Not Publish",
                    "Redacted",
                    ["a2", "a3"],
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("adviceId", T.IntegerType(), True),
                    T.StructField("adviceReference", T.StringType(), True),
                    T.StructField("caseId", T.StringType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("title", T.StringType(), True),
                    T.StructField("titleWelsh", T.StringType(), True),
                    T.StructField("from", T.StringType(), True),
                    T.StructField("agent", T.StringType(), True),
                    T.StructField("method", T.StringType(), True),
                    T.StructField("enquiryDate", T.StringType(), True),
                    T.StructField("enquiryDetails", T.StringType(), True),
                    T.StructField("enquiryDetailsWelsh", T.StringType(), True),
                    T.StructField("adviceGivenBy", T.StringType(), True),
                    T.StructField("adviceDate", T.StringType(), True),
                    T.StructField("adviceDetails", T.StringType(), True),
                    T.StructField("adviceDetailsWelsh", T.StringType(), True),
                    T.StructField("status", T.StringType(), True),
                    T.StructField("redactionStatus", T.StringType(), True),
                    T.StructField("attachmentIds", T.ArrayType(T.StringType()), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )
        harmonised_s51_advice_table = f"{test_case}_nsip_s51_advice"
        self.write_existing_table(
            spark,
            harmonised_s51_advice,
            harmonised_s51_advice_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_s51_advice_table,
            "overwrite",
        )

        s51_advice_table = f"{test_case}_s51_advice"

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_s51_advice_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipS51AdviceCuratedProcess,
                "HARMONISED_TABLE",
                f"odw_harmonised_db.{harmonised_s51_advice_table}",
            ),
            mock.patch.object(
                NsipS51AdviceCuratedProcess, "OUTPUT_TABLE", s51_advice_table
            ),
        ):
            inst = NsipS51AdviceCuratedProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_s51_advice",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{s51_advice_table}")

        rows = {
            row["adviceId"]: row.asDict(recursive=True) for row in actual_df.collect()
        }

        assert actual_df.count() == 2

        assert rows[1]["caseId"] in (-1, "-1")
        assert rows[1]["agent"] is None
        assert rows[1]["method"] == "email"
        assert rows[1]["status"] == "unchecked"

        assert rows[2]["caseId"] in (123, "123")
        assert rows[2]["agent"] == "Agent Ltd"
        assert rows[2]["method"] is None
        assert rows[2]["status"] == "donotpublish"
