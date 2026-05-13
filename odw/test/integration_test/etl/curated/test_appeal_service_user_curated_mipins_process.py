import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType, StringType, StructField, StructType
from odw.core.etl.transformation.curated.appeal_service_user_curated_mipins_process import AppealServiceUserCuratedMipinsProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal, assert_etl_result_successful
from odw.test.util.session_util import PytestSparkSessionUtil

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


class TestAppealServiceUserCuratedMipinsProcess(ETLTestCase):
    def compare_curated_data(expected_data: DataFrame, actual_data: DataFrame):
        assert_dataframes_equal(expected_data, actual_data)

    def test__appeal_service_user_curated_mipins_process__run__with_valid_and_filtered_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_asucmp_r_wvfr"

        sb_service_user_table_name = f"{test_case}_sb_service_user"
        service_user_table_name = f"{test_case}_service_user"
        output_table_name = f"{test_case}_appeal_service_user_curated_mipins"

        sb_service_user = spark.createDataFrame(
            [
                _sb_service_user_row(id="SB-VALID", ingestionDate="2024-01-01 00:00:00", ValidTo=None),
                _sb_service_user_row(id="SB-NULL-INGESTION", ingestionDate=None, ValidTo=None),
                _sb_service_user_row(id="SB-DROP-OLD-INGESTION", ingestionDate="1899-12-31 00:00:00", ValidTo=None),
                _sb_service_user_row(id="SB-DROP-OLD-VALIDTO", ingestionDate="2024-01-01 00:00:00", ValidTo="1899-12-31 00:00:00"),
            ],
            schema=_source_service_user_schema(),
        )

        service_user = spark.createDataFrame(
            [
                _service_user_row(id="SU-VALID", ingestionDate="2024-02-01 00:00:00", ValidTo=None),
                _service_user_row(id="SU-1900-VALIDTO", ingestionDate="2024-02-01 00:00:00", ValidTo="1900-01-01 00:00:00"),
                _service_user_row(id="SU-DROP-OLD-INGESTION", ingestionDate="1899-12-31 00:00:00", ValidTo=None),
                _service_user_row(id="SU-DROP-OLD-VALIDTO", ingestionDate="2024-02-01 00:00:00", ValidTo="1899-12-31 00:00:00"),
            ],
            schema=_source_service_user_schema(),
        )

        self.write_existing_table(
            spark,
            sb_service_user,
            sb_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            sb_service_user_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            service_user,
            service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            service_user_table_name,
            "overwrite",
        )

        expected_data = spark.createDataFrame(
            [
                (
                    "SB-VALID",
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
                ),
                (
                    "SB-NULL-INGESTION",
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
                    None,
                    None,
                    "",
                    "Y",
                ),
                (
                    "SU-VALID",
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
                ),
                (
                    "SU-1900-VALIDTO",
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
                    "1900-01-01 00:00:00",
                    "",
                    "Y",
                ),
            ],
            schema=_output_schema(),
        )

        with (
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "SOURCE_SB_SERVICE_USER_TABLE", sb_service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "SOURCE_SERVICE_USER_TABLE", service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "OUTPUT_TABLE", output_table_name),
        ):
            inst = AppealServiceUserCuratedMipinsProcess(spark)
            result = inst.run()

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_curated_db.{output_table_name}")
            self.compare_curated_data(expected_data, actual_table_data)

    def test__appeal_service_user_curated_mipins_process__run__uses_union_not_union_all_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        test_case = "t_asucmp_r_un"

        sb_service_user_table_name = f"{test_case}_sb_service_user"
        service_user_table_name = f"{test_case}_service_user"
        output_table_name = f"{test_case}_appeal_service_user_curated_mipins"

        duplicate_row = _sb_service_user_row(id="DUP-001")

        sb_service_user = spark.createDataFrame([duplicate_row], schema=_source_service_user_schema())
        service_user = spark.createDataFrame([duplicate_row], schema=_source_service_user_schema())

        self.write_existing_table(
            spark,
            sb_service_user,
            sb_service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            sb_service_user_table_name,
            "overwrite",
        )
        self.write_existing_table(
            spark,
            service_user,
            service_user_table_name,
            "odw_harmonised_db",
            "odw-harmonised",
            service_user_table_name,
            "overwrite",
        )

        expected_data = spark.createDataFrame(
            [
                (
                    "DUP-001",
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
                ),
            ],
            schema=_output_schema(),
        )

        with (
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "SOURCE_SB_SERVICE_USER_TABLE", sb_service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "SOURCE_SERVICE_USER_TABLE", service_user_table_name),
            mock.patch.object(AppealServiceUserCuratedMipinsProcess, "OUTPUT_TABLE", output_table_name),
        ):
            inst = AppealServiceUserCuratedMipinsProcess(spark)
            result = inst.run()

            assert_etl_result_successful(result)

            actual_table_data = spark.table(f"odw_curated_db.{output_table_name}")
            self.compare_curated_data(expected_data, actual_table_data)
