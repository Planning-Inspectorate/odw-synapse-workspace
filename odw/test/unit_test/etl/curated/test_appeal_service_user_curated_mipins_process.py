import mock
import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_service_user_curated_mipins_process import AppealServiceUserCuratedMipinsProcess
from odw.core.etl.metadata_manager import MetadataManager
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.test_case import SparkTestCase

pytestmark = pytest.mark.xfail(reason="Curation logic not implemented yet")


def _source_service_user_schema():
    return StructType(
        [
            StructField("ServiceUserID", LongType(), True),
            StructField("id", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("FirstName", StringType(), True),
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
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSUID", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("ingestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _output_schema():
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("caseReference", StringType(), True),
            StructField("salutation", StringType(), True),
            StructField("FirstName", StringType(), True),
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
            StructField("sourceSystem", StringType(), True),
            StructField("sourceSUID", StringType(), True),
            StructField("ODTSourceSystem", StringType(), True),
            StructField("ingestionDate", StringType(), True),
            StructField("ValidTo", StringType(), True),
            StructField("RowID", StringType(), True),
            StructField("IsActive", StringType(), True),
        ]
    )


def _sb_service_user_row(**overrides):
    row = {
        "ServiceUserID": 1,
        "id": "SB-001",
        "caseReference": "APP-001",
        "salutation": "Mr",
        "FirstName": "Service",
        "lastName": "Bus",
        "addressLine1": "1 Service Street",
        "addressLine2": "Flat 1",
        "addressTown": "Bristol",
        "addressCounty": "Somerset",
        "postcode": "BS1 1AA",
        "addressCountry": "United Kingdom",
        "organisation": "Service Org",
        "organisationType": "Company",
        "role": "Owner",
        "telephoneNumber": "111",
        "otherPhoneNumber": "222",
        "faxNumber": "333",
        "emailAddress": "service@example.com",
        "webAddress": "https://service.example.com",
        "serviceUserType": "Appellant",
        "sourceSystem": "Back Office",
        "sourceSUID": "SB-001",
        "ODTSourceSystem": "Back Office",
        "ingestionDate": "2024-01-01 00:00:00",
        "ValidTo": None,
        "RowID": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


def _service_user_row(**overrides):
    row = {
        "ServiceUserID": 2,
        "id": "SU-001",
        "caseReference": "APP-002",
        "salutation": "Ms",
        "FirstName": "Harmonised",
        "lastName": "User",
        "addressLine1": "2 Harmonised Street",
        "addressLine2": "Flat 2",
        "addressTown": "London",
        "addressCounty": "Greater London",
        "postcode": "SW1A 1AA",
        "addressCountry": "United Kingdom",
        "organisation": "Harmonised Org",
        "organisationType": "Charity",
        "role": "Agent",
        "telephoneNumber": "444",
        "otherPhoneNumber": "555",
        "faxNumber": "666",
        "emailAddress": "harmonised@example.com",
        "webAddress": "https://harmonised.example.com",
        "serviceUserType": "Agent",
        "sourceSystem": "Horizon",
        "sourceSUID": "SU-001",
        "ODTSourceSystem": "Horizon",
        "ingestionDate": "2024-02-01 00:00:00",
        "ValidTo": None,
        "RowID": "",
        "IsActive": "Y",
    }
    row.update(overrides)
    return row


def _source_data(spark, sb_rows=None, service_user_rows=None):
    return {
        "sb_service_user_data": spark.createDataFrame(sb_rows or [], schema=_source_service_user_schema()),
        "service_user_data": spark.createDataFrame(service_user_rows or [], schema=_source_service_user_schema()),
        "target_exists": False,
    }


class TestAppealServiceUserCuratedMipinsProcess(SparkTestCase):
    def compare_curated_data(self, expected_data, actual_data):
        assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_service_user_curated_mipins_process__get_name__returns_expected_name(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        inst = AppealServiceUserCuratedMipinsProcess(spark)

        assert inst.get_name() == "appeal_service_user_curated_mipins_process"

    def test__appeal_service_user_curated_mipins_process__process__outputs_expected_legacy_columns_only(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, sb_rows=[_sb_service_user_row()])

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.columns == [
            "id",
            "caseReference",
            "salutation",
            "FirstName",
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
            "sourceSystem",
            "sourceSUID",
            "ODTSourceSystem",
            "ingestionDate",
            "ValidTo",
            "RowID",
            "IsActive",
        ]

    def test__appeal_service_user_curated_mipins_process__process__maps_sb_service_user_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, sb_rows=[_sb_service_user_row()])

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        actual_data = data_to_write[inst.OUTPUT_TABLE]["data"]

        expected_data = spark.createDataFrame(
            [
                (
                    "SB-001",
                    "APP-001",
                    "Mr",
                    "Service",
                    "Bus",
                    "1 Service Street",
                    "Flat 1",
                    "Bristol",
                    "Somerset",
                    "BS1 1AA",
                    "United Kingdom",
                    "Service Org",
                    "Company",
                    "Owner",
                    "111",
                    "222",
                    "333",
                    "service@example.com",
                    "https://service.example.com",
                    "Appellant",
                    "Back Office",
                    "SB-001",
                    "Back Office",
                    "2024-01-01 00:00:00",
                    None,
                    "",
                    "Y",
                )
            ],
            schema=_output_schema(),
        )

        self.compare_curated_data(expected_data, actual_data)
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_service_user_curated_mipins_process__process__maps_service_user_columns_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, service_user_rows=[_service_user_row()])

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        actual_data = data_to_write[inst.OUTPUT_TABLE]["data"]

        expected_data = spark.createDataFrame(
            [
                (
                    "SU-001",
                    "APP-002",
                    "Ms",
                    "Harmonised",
                    "User",
                    "2 Harmonised Street",
                    "Flat 2",
                    "London",
                    "Greater London",
                    "SW1A 1AA",
                    "United Kingdom",
                    "Harmonised Org",
                    "Charity",
                    "Agent",
                    "444",
                    "555",
                    "666",
                    "harmonised@example.com",
                    "https://harmonised.example.com",
                    "Agent",
                    "Horizon",
                    "SU-001",
                    "Horizon",
                    "2024-02-01 00:00:00",
                    None,
                    "",
                    "Y",
                )
            ],
            schema=_output_schema(),
        )

        self.compare_curated_data(expected_data, actual_data)
        assert result.metadata.insert_count == 1
        assert result.metadata.update_count == 0

    def test__appeal_service_user_curated_mipins_process__process__unions_sb_and_service_user_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            sb_rows=[_sb_service_user_row(id="SB-001")],
            service_user_rows=[_service_user_row(id="SU-001")],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 2
        assert df.where(F.col("id") == "SB-001").count() == 1
        assert df.where(F.col("id") == "SU-001").count() == 1
        assert result.metadata.insert_count == 2

    def test__appeal_service_user_curated_mipins_process__process__uses_union_not_union_all_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        duplicate_row = _sb_service_user_row(id="DUP-001")

        source_data = _source_data(
            spark,
            sb_rows=[duplicate_row],
            service_user_rows=[duplicate_row],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 1
        assert df.where(F.col("id") == "DUP-001").count() == 1
        assert result.metadata.insert_count == 1

    def test__appeal_service_user_curated_mipins_process__process__filters_out_ingestion_dates_before_1900_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            sb_rows=[
                _sb_service_user_row(id="KEEP-NULL-INGESTION", ingestionDate=None),
                _sb_service_user_row(id="KEEP-1900-INGESTION", ingestionDate="1900-01-01 00:00:00"),
                _sb_service_user_row(id="KEEP-AFTER-1900-INGESTION", ingestionDate="2024-01-01 00:00:00"),
                _sb_service_user_row(id="DROP-BEFORE-1900-INGESTION", ingestionDate="1899-12-31 00:00:00"),
            ],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "KEEP-NULL-INGESTION").count() == 1
        assert df.where(F.col("id") == "KEEP-1900-INGESTION").count() == 1
        assert df.where(F.col("id") == "KEEP-AFTER-1900-INGESTION").count() == 1
        assert df.where(F.col("id") == "DROP-BEFORE-1900-INGESTION").count() == 0
        assert result.metadata.insert_count == 3

    def test__appeal_service_user_curated_mipins_process__process__filters_out_valid_to_dates_before_1900_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[
                _service_user_row(id="KEEP-NULL-VALIDTO", ValidTo=None),
                _service_user_row(id="KEEP-1900-VALIDTO", ValidTo="1900-01-01 00:00:00"),
                _service_user_row(id="KEEP-AFTER-1900-VALIDTO", ValidTo="2024-01-01 00:00:00"),
                _service_user_row(id="DROP-BEFORE-1900-VALIDTO", ValidTo="1899-12-31 00:00:00"),
            ],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "KEEP-NULL-VALIDTO").count() == 1
        assert df.where(F.col("id") == "KEEP-1900-VALIDTO").count() == 1
        assert df.where(F.col("id") == "KEEP-AFTER-1900-VALIDTO").count() == 1
        assert df.where(F.col("id") == "DROP-BEFORE-1900-VALIDTO").count() == 0
        assert result.metadata.insert_count == 3

    def test__appeal_service_user_curated_mipins_process__process__nullifies_blank_valid_to_for_sb_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            sb_rows=[_sb_service_user_row(id="SB-BLANK-VALIDTO", ValidTo="")],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["id"] == "SB-BLANK-VALIDTO"
        assert row["ValidTo"] is None

    def test__appeal_service_user_curated_mipins_process__process__does_not_nullify_blank_valid_to_for_service_user_rows_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            service_user_rows=[_service_user_row(id="SU-BLANK-VALIDTO", ValidTo="")],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, _ = inst.process(source_data=source_data)

        row = data_to_write[inst.OUTPUT_TABLE]["data"].collect()[0]

        assert row["id"] == "SU-BLANK-VALIDTO"
        assert row["ValidTo"] == ""

    def test__appeal_service_user_curated_mipins_process__process__does_not_filter_on_is_active_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            sb_rows=[
                _sb_service_user_row(id="ACTIVE-SB", IsActive="Y"),
                _sb_service_user_row(id="INACTIVE-SB", IsActive="N"),
            ],
            service_user_rows=[
                _service_user_row(id="ACTIVE-SU", IsActive="Y"),
                _service_user_row(id="INACTIVE-SU", IsActive="N"),
            ],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.where(F.col("id") == "ACTIVE-SB").count() == 1
        assert df.where(F.col("id") == "INACTIVE-SB").count() == 1
        assert df.where(F.col("id") == "ACTIVE-SU").count() == 1
        assert df.where(F.col("id") == "INACTIVE-SU").count() == 1
        assert result.metadata.insert_count == 4

    def test__appeal_service_user_curated_mipins_process__process__empty_sources_return_empty_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark)

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        assert df.count() == 0
        assert data_to_write[inst.OUTPUT_TABLE]["write_mode"] == "overwrite"
        assert result.metadata.insert_count == 0
        assert result.metadata.update_count == 0

    def test__appeal_service_user_curated_mipins_process__process__uses_overwrite_and_parquet_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, sb_rows=[_sb_service_user_row()])

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        write_config = data_to_write[inst.OUTPUT_TABLE]
        df = write_config["data"]

        assert df.count() == 1
        assert write_config["write_mode"] == "overwrite"
        assert write_config["file_format"] == "parquet"
        assert result.metadata.insert_count == 1

    def test__appeal_service_user_curated_mipins_process__run__initial_load_matches_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()

        source_data = _source_data(
            spark,
            sb_rows=[_sb_service_user_row(id="SB-001")],
            service_user_rows=[_service_user_row(id="SU-001")],
        )

        with mock.patch("odw.core.etl.etl_process.LoggingUtil") as mock_logging:
            mock_logging.return_value = mock.Mock()

            inst = AppealServiceUserCuratedMipinsProcess(spark)

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

    def test__appeal_service_user_curated_mipins_process__process__blank_valid_to_differs_between_sources_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(
            spark,
            sb_rows=[_sb_service_user_row(id="SB-BLANK-VALIDTO", ValidTo="")],
            service_user_rows=[_service_user_row(id="SU-BLANK-VALIDTO", ValidTo="")],
        )

        inst = AppealServiceUserCuratedMipinsProcess(spark)
        data_to_write, result = inst.process(source_data=source_data)

        df = data_to_write[inst.OUTPUT_TABLE]["data"]

        sb_row = df.where(F.col("id") == "SB-BLANK-VALIDTO").collect()[0]
        su_row = df.where(F.col("id") == "SU-BLANK-VALIDTO").collect()[0]

        assert sb_row["ValidTo"] is None
        assert su_row["ValidTo"] == ""
        assert result.metadata.insert_count == 2
