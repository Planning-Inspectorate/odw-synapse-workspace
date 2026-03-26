from odw.test.util.mock.import_mock_notebook_utils import notebookutils
from odw.core.etl.transformation.curated.nsip_subscription_curated_process import NsipSubscriptionCuratedProcess
from odw.test.util.session_util import PytestSparkSessionUtil
import pyspark.sql.types as T
import mock


def test__nsip_subscription_curated_process__run__selects_and_deduplicates_rows():
    spark = PytestSparkSessionUtil().get_spark_session()

    harmonised_subscriptions = spark.createDataFrame(
        [
            (1, "EN010001", "a@test.com", "all", "2025-01-01", None, "en"),
            (1, "EN010001", "a@test.com", "all", "2025-01-01", None, "en"),
            (2, "EN010002", "b@test.com", "documents", "2025-02-01", "2025-03-01", "cy"),
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
            ]
        ),
    )

    source_data = {
        "harmonised_subscriptions": harmonised_subscriptions,
    }

    with mock.patch(
            "odw.core.etl.transformation.curated.nsip_subscription_curated_process.Util.get_storage_account",
            return_value="test_storage",
    ), mock.patch(
        "odw.core.etl.etl_process.LoggingUtil"
    ) as MockEtlLogging, mock.patch(
        "odw.core.etl.transformation.curated.nsip_subscription_curated_process.LoggingUtil"
    ) as MockProcessLogging:
        MockEtlLogging.return_value = mock.Mock()
        MockProcessLogging.return_value = mock.Mock()

        inst = NsipSubscriptionCuratedProcess(spark)

        with mock.patch.object(inst, "load_data", return_value=source_data), \
                mock.patch.object(inst, "write_data") as mock_write:
            result = inst.run()

    data_to_write = mock_write.call_args[0][0]
    actual_df = data_to_write[inst.OUTPUT_TABLE]["data"]

    assert actual_df.count() == 2
    assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
    assert data_to_write[inst.OUTPUT_TABLE]["table_name"] == "nsip_subscription"
    assert result.metadata.insert_count == 2