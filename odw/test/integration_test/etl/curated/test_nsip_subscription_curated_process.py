import odw.test.util.mock_util.import_mock_notebook_utils  # noqa: F401
from odw.core.etl.transformation.curated.nsip_subscription_curated_process import (
    NsipSubscriptionCuratedProcess,
)
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_etl_result_successful
import pyspark.sql.types as T
import mock


class TestNSIPSubscriptionCurated(ETLTestCase):
    def test__nsip_subscription_curated_process__run__selects_and_deduplicates_rows(
        self,
    ):
        test_case = "t_nscp_r_sadr"
        spark = PytestSparkSessionUtil().get_spark_session()

        harmonised_subscriptions = spark.createDataFrame(
            [
                (1, "EN010001", "a@test.com", "all", "2025-01-01", None, "en", "Y"),
                (1, "EN010001", "a@test.com", "all", "2025-01-01", None, "en", "Y"),
                (
                    2,
                    "EN010002",
                    "b@test.com",
                    "documents",
                    "2025-02-01",
                    "2025-03-01",
                    "cy",
                    "Y",
                ),
            ],
            T.StructType(
                [
                    T.StructField("subscriptionId", T.IntegerType(), True),
                    T.StructField("caseReference", T.StringType(), True),
                    T.StructField("emailAddress", T.StringType(), True),
                    T.StructField("subscriptionType", T.StringType(), True),
                    T.StructField("startDate", T.StringType(), True),
                    T.StructField("endDate", T.StringType(), True),
                    T.StructField("language", T.StringType(), True),
                    T.StructField("IsActive", T.StringType(), True),
                ]
            ),
        )

        harmonised_subscriptions_table = f"{test_case}_sb_nsip_subscription"
        self.write_existing_table(
            spark,
            harmonised_subscriptions,
            harmonised_subscriptions_table,
            "odw_harmonised_db",
            "odw-harmonised",
            harmonised_subscriptions_table,
            "overwrite",
        )
        nsip_subscription = f"{test_case}_nsip_subscription"

        with (
            mock.patch(
                "odw.core.etl.transformation.curated.nsip_subscription_curated_process.Util.get_storage_account",
                return_value="test_storage",
            ),
            mock.patch.object(
                NsipSubscriptionCuratedProcess,
                "HARMONISED_TABLE",
                f"odw_harmonised_db.{harmonised_subscriptions_table}",
            ),
            mock.patch.object(
                NsipSubscriptionCuratedProcess, "OUTPUT_TABLE", nsip_subscription
            ),
        ):
            inst = NsipSubscriptionCuratedProcess(spark)

            result = inst.run(
                orchestration_run_id=test_case,
                orchestration_entity_name="nsip_subscription",
                orchestration_stage_name="curate",
            )
            assert_etl_result_successful(result)

        actual_df = spark.table(f"odw_curated_db.{nsip_subscription}")

        assert actual_df.count() == 2
