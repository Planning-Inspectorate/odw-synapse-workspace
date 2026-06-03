import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_service_user_curated_process import AppealServiceUserCuratedProcess
from odw.core.etl.metadata_manager import MetadataManager
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curation logic not implemented yet")


def _harmonised_service_user_schema():
    return StructType(
        [
            StructField("ServiceUserID", LongType(), True),
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("addressCountry", StringType(), True),
            StructField("organisation", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("role", StringType(), True),
            StructField("telephoneNumber", StringType(), True),
            StructField("otherPhoneNumber", StringType(), True),
            StructField("faxNumber", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("webAddress", StringType(), True),
            StructField("serviceUserType", StringType(), True),
            StructField("serviceUserTypeInternal", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("contactMethod", StringType(), True),
            StructField("Migrated", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("IngestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _curated_appeal_service_user_schema():
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("addressLine1", StringType(), True),
            StructField("addressLine2", StringType(), True),
            StructField("addressTown", StringType(), True),
            StructField("addressCounty", StringType(), True),
            StructField("postcode", StringType(), True),
            StructField("addressCountry", StringType(), True),
            StructField("organisation", StringType(), True),
            StructField("organisationType", StringType(), True),
            StructField("role", StringType(), True),
            StructField("telephoneNumber", StringType(), True),
            StructField("otherPhoneNumber", StringType(), True),
            StructField("faxNumber", StringType(), True),
            StructField("emailAddress", StringType(), True),
            StructField("webAddress", StringType(), True),
            StructField("serviceUserType", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("sourceSuid", StringType(), True),
            StructField("sourceSystem", StringType(), True),
        ]
    )


def _harmonised_service_user_row(**overrides):
    row = {
        "ServiceUserID": 1,
        "id": "SU-001",
        "salutation": "Mr",
        "firstName": "Appeal",
        "lastName": "User",
        "addressLine1": "1 Appeal Street",
        "addressLine2": "Flat 1",
        "addressTown": "Bristol",
        "addressCounty": "Somerset",
        "postcode": "BS1 1AA",
        "addressCountry": "United Kingdom",
        "organisation": "Appeal Org",
        "organisationType": "Company",
        "role": "Owner",
        "telephoneNumber": "111",
        "otherPhoneNumber": "222",
        "faxNumber": "333",
        "emailAddress": "appeal@example.com",
        "webAddress": "https://example.com",
        "serviceUserType": "Appellant",
        "serviceUserTypeInternal": "Appellant",
        "caseReference": "APP-001",
        "sourceSystem": "Horizon",
        "sourceSuid": "SU-001",
        "contactMethod": None,
        "Migrated": "0",
        "ODTSourceSystem": "Horizon",
        "IngestionDate": "2024-01-01 00:00:00",
        "ValidTo": None,
        "RowID": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


def _source_data(spark, service_user_rows=None):
    return {
        "service_user_data": spark.createDataFrame(
            service_user_rows or [],
            schema=_harmonised_service_user_schema(),
        ),
    }


class TestAppealServiceUserCuratedProcess(SparkTestCase):
    def test__appeal_service_user_curated_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = AppealServiceUserCuratedProcess(spark)

        assert inst.get_name() == "Appeal Service User Curation Process"

    def test__appeal_service_user_curated_process__process__outputs_expected_legacy_columns_only(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_user_rows=[_harmonised_service_user_row()])

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "id",
            "salutation",
            "firstName",
            "lastName",
            "addressLine1",
            "addressLine2",
            "addressTown",
            "addressCounty",
            "postcode",
            "addressCountry",
            "organisation",
            "organisationType",
            "role",
            "telephoneNumber",
            "otherPhoneNumber",
            "faxNumber",
            "emailAddress",
            "webAddress",
            "serviceUserType",
            "caseReference",
            "sourceSuid",
            "sourceSystem",
        ]

    def test__appeal_service_user_curated_process__process__maps_selected_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_user_rows=[_harmonised_service_user_row()])

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]
        row = df.collect()[0]

        assert row["id"] == "SU-001"
        assert row["salutation"] == "Mr"
        assert row["firstName"] == "Appeal"
        assert row["lastName"] == "User"
        assert row["addressLine1"] == "1 Appeal Street"
        assert row["addressLine2"] == "Flat 1"
        assert row["addressTown"] == "Bristol"
        assert row["addressCounty"] == "Somerset"
        assert row["postcode"] == "BS1 1AA"
        assert row["addressCountry"] == "United Kingdom"
        assert row["organisation"] == "Appeal Org"
        assert row["organisationType"] == "Company"
        assert row["role"] == "Owner"
        assert row["telephoneNumber"] == "111"
        assert row["otherPhoneNumber"] == "222"
        assert row["faxNumber"] == "333"
        assert row["emailAddress"] == "appeal@example.com"
        assert row["webAddress"] == "https://example.com"
        assert row["serviceUserType"] == "Appellant"
        assert row["caseReference"] == "APP-001"
        assert row["sourceSuid"] == "SU-001"
        assert row["sourceSystem"] == "Horizon"

        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_service_user_curated_process__process__includes_appellant_from_any_source_system_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="APP-HZN",
                    serviceUserType="Appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="APP-BO",
                    serviceUserType="Appellant",
                    sourceSystem="Back Office",
                ),
                _harmonised_service_user_row(
                    id="APP-OTHER",
                    serviceUserType="Appellant",
                    sourceSystem="Some Other Source",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "APP-HZN").count() == 1
        assert df.where(F.col("id") == "APP-BO").count() == 1
        assert df.where(F.col("id") == "APP-OTHER").count() == 1
        assert result.metadata.insert_count == 3

    def test__appeal_service_user_curated_process__process__includes_back_office_appeals_source_system_for_any_service_user_type_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="BO-AGENT",
                    serviceUserType="Agent",
                    sourceSystem="back-office-appeals",
                ),
                _harmonised_service_user_row(
                    id="BO-REP",
                    serviceUserType="RepresentationContact",
                    sourceSystem="back-office-appeals",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "BO-AGENT").count() == 1
        assert df.where(F.col("id") == "BO-REP").count() == 1
        assert result.metadata.insert_count == 2

    def test__appeal_service_user_curated_process__process__filters_out_non_appellant_non_back_office_appeals_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="KEEP-APP",
                    serviceUserType="Appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="KEEP-BO",
                    serviceUserType="Agent",
                    sourceSystem="back-office-appeals",
                ),
                _harmonised_service_user_row(
                    id="DROP-HZN-AGENT",
                    serviceUserType="Agent",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="DROP-REP",
                    serviceUserType="RepresentationContact",
                    sourceSystem="Horizon",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "KEEP-APP").count() == 1
        assert df.where(F.col("id") == "KEEP-BO").count() == 1
        assert df.where(F.col("id") == "DROP-HZN-AGENT").count() == 0
        assert df.where(F.col("id") == "DROP-REP").count() == 0
        assert result.metadata.insert_count == 2

    def test__appeal_service_user_curated_process__process__filter_uses_or_not_and_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="APP-NOT-BACK-OFFICE-APPEALS",
                    serviceUserType="Appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="BACK-OFFICE-APPEALS-NOT-APP",
                    serviceUserType="Agent",
                    sourceSystem="back-office-appeals",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "APP-NOT-BACK-OFFICE-APPEALS").count() == 1
        assert df.where(F.col("id") == "BACK-OFFICE-APPEALS-NOT-APP").count() == 1

    def test__appeal_service_user_curated_process__process__preserves_duplicate_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        duplicate_row = _harmonised_service_user_row(id="DUP-001")

        source_data = _source_data(
            spark,
            service_user_rows=[
                duplicate_row,
                duplicate_row,
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        # Legacy notebook does not use DISTINCT so duplicate eligible source rows are preserved
        assert df.count() == 2
        assert df.where(F.col("id") == "DUP-001").count() == 2
        assert result.metadata.insert_count == 2

    def test__appeal_service_user_curated_process__process__does_not_filter_on_is_active_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="ACTIVE-APP",
                    serviceUserType="Appellant",
                    IsActive="Y",
                ),
                _harmonised_service_user_row(
                    id="INACTIVE-APP",
                    serviceUserType="Appellant",
                    IsActive="N",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "ACTIVE-APP").count() == 1
        assert df.where(F.col("id") == "INACTIVE-APP").count() == 1
        assert result.metadata.insert_count == 2

    def test__appeal_service_user_curated_process__process__empty_source_returns_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark)

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__appeal_service_user_curated_process__process__uses_overwrite_and_parquet_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_user_rows=[_harmonised_service_user_row()])

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 1
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 1

    def test__appeal_service_user_curated_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="APP-001",
                    serviceUserType="Appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="BO-AGENT",
                    serviceUserType="Agent",
                    sourceSystem="back-office-appeals",
                ),
                _harmonised_service_user_row(
                    id="DROP-HZN-AGENT",
                    serviceUserType="Agent",
                    sourceSystem="Horizon",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
            mock.patch.object(MetadataManager, "__init__", return_value=None),
            mock.patch.object(MetadataManager, "create", return_value=None),
            mock.patch.object(MetadataManager, "update", return_value=None),
        ):
            result = inst.run()

        data_to_write = mock_write.call_args[0][0]
        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert data_to_write[inst.OUTPUT_TABLE]["file_format"] == "parquet"
        assert result.metadata.insert_count == 2
        assert result.metadata.update_count == 0

        assert df.where(F.col("id") == "APP-001").count() == 1
        assert df.where(F.col("id") == "BO-AGENT").count() == 1
        assert df.where(F.col("id") == "DROP-HZN-AGENT").count() == 0

    def test__appeal_service_user_curated_process__process__filters_are_case_sensitive_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _harmonised_service_user_row(
                    id="KEEP-APP",
                    serviceUserType="Appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="DROP-LOWER-APP",
                    serviceUserType="appellant",
                    sourceSystem="Horizon",
                ),
                _harmonised_service_user_row(
                    id="KEEP-BO",
                    serviceUserType="Agent",
                    sourceSystem="back-office-appeals",
                ),
                _harmonised_service_user_row(
                    id="DROP-UPPER-BO",
                    serviceUserType="Agent",
                    sourceSystem="BACK-OFFICE-APPEALS",
                ),
                _harmonised_service_user_row(
                    id="DROP-TITLE-BO",
                    serviceUserType="Agent",
                    sourceSystem="Back Office Appeals",
                ),
            ],
        )

        inst = AppealServiceUserCuratedProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "KEEP-APP").count() == 1
        assert df.where(F.col("id") == "KEEP-BO").count() == 1
        assert df.where(F.col("id") == "DROP-LOWER-APP").count() == 0
        assert df.where(F.col("id") == "DROP-UPPER-BO").count() == 0
        assert df.where(F.col("id") == "DROP-TITLE-BO").count() == 0
        assert result.metadata.insert_count == 2
