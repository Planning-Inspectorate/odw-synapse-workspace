"""
Unit tests for anonymisation of inspector data in the harmonised layer.

Covers the PII fields present in odw_harmonised_db.pins_inspector:
  - firstName, lastName       → NameMaskStrategy  (keep first letter, mask rest)
  - inspectorManager          → NameMaskStrategy  (full-name variant: keep first/last letter of each word)
  - email                     → EmailMaskStrategy (SHA-256 of lowercased value)
  - address (struct)          → AddressStrategy   (REDACTED for all fields except postcode outward code)

Non-PII fields (sapId, entraId, grade, isActive, fte, unit, service, group, title,
sourceSystem, validFrom, specialisms) must remain unchanged.
"""

from unittest import mock
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from odw.core.anonymisation import AnonymisationEngine
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase

_ASSET_QUALIFIED_NAME = "https://dummy.dfs.core.windows.net/odw-raw/entraid/pins-inspector/{Year}-{Month}-{Day}/pins-inspector.json"
_APPLY_FROM_PURVIEW_KWARGS = dict(
    purview_name="dummy-pview",
    tenant_id="dummy-tenant",
    client_id="dummy-client",
    client_secret="dummy-secret",
    asset_type_name="azure_datalake_gen2_resource_set",
    asset_qualified_name=_ASSET_QUALIFIED_NAME,
)

_ADDRESS_SCHEMA = StructType(
    [
        StructField("addressLine1", StringType(), True),
        StructField("addressLine2", StringType(), True),
        StructField("townCity", StringType(), True),
        StructField("county", StringType(), True),
        StructField("postcode", StringType(), True),
    ]
)


def _apply(spark, data, schema, mocked_cols):
    df = spark.createDataFrame(data, schema) if schema else spark.createDataFrame(data)
    engine = AnonymisationEngine()
    with mock.patch(
        "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
        return_value=mocked_cols,
    ):
        return engine.apply_from_purview(df, **_APPLY_FROM_PURVIEW_KWARGS)


class TestInspectorHarmonisedAnonymisation(SparkTestCase):
    def test__inspector__first_name_is_masked_to_first_letter(self):
        """firstName is masked: first letter kept, remaining characters replaced with asterisks."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [
            {"sapId": "S001", "firstName": "John"},
            {"sapId": "S002", "firstName": "Charlotte"},
        ]
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        rows = out.orderBy("sapId").select("sapId", "firstName").collect()

        assert rows[0]["firstName"] == "J***"
        assert rows[1]["firstName"] == "C********"

    def test__inspector__last_name_is_masked_to_first_letter(self):
        """lastName is masked: first letter kept, remaining characters replaced with asterisks."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [
            {"sapId": "S001", "lastName": "Doe"},
            {"sapId": "S002", "lastName": "Smith"},
        ]
        mocked_cols = [
            {"column_name": "lastName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        rows = out.orderBy("sapId").select("sapId", "lastName").collect()

        assert rows[0]["lastName"] == "D**"
        assert rows[1]["lastName"] == "S****"

    def test__inspector__manager_full_name_masks_first_and_last_letter(self):
        """inspectorManager uses full-name masking: first letter of first name + last letter of last name."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [
            {"sapId": "S001", "inspectorManager": "Jane Smith"},
            {"sapId": "S002", "inspectorManager": "Robert Brown"},
        ]
        mocked_cols = [
            {"column_name": "inspectorManager", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        rows = out.orderBy("sapId").select("sapId", "inspectorManager").collect()

        assert rows[0]["inspectorManager"] == "J*** ****h"
        assert rows[1]["inspectorManager"] == "R***** ****n"

    def test__inspector__email_is_sha256_hashed(self):
        """email is SHA-256 hashed (lowercased before hashing for case-insensitive joins)."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [
            {"sapId": "S001", "email": "john.doe@example.com"},
            {"sapId": "S002", "email": "Jane.Smith@example.com"},
        ]
        mocked_cols = [
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        rows = out.orderBy("sapId").select("sapId", "email").collect()

        # sha256("john.doe@example.com")
        assert rows[0]["email"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"
        # sha256("jane.smith@example.com") — email is lowercased before hashing
        assert rows[1]["email"] == "f2d1f1c853fd1f4be1eb5060eaae93066c877d069473795e31db5e70c4880859"

    def test__inspector__address_struct_is_redacted_preserving_postcode_outward_code(self):
        """address struct fields are REDACTED; postcode keeps the outward code (before the space)."""
        spark = PytestSparkSessionUtil().get_spark_session()

        schema = StructType(
            [
                StructField("sapId", StringType(), True),
                StructField("address", _ADDRESS_SCHEMA, True),
            ]
        )
        data = [
            {
                "sapId": "S001",
                "address": {
                    "addressLine1": "15 Lambourne Road",
                    "addressLine2": "Flat 3",
                    "townCity": "London",
                    "county": "Greater London",
                    "postcode": "SW1A 1AA",
                },
            },
            {
                "sapId": "S002",
                "address": {
                    "addressLine1": "7 Oak Street",
                    "addressLine2": None,
                    "townCity": "Bristol",
                    "county": "Avon",
                    "postcode": "BS1 4ST",
                },
            },
        ]
        mocked_cols = [
            {"column_name": "address", "classifications": ["MICROSOFT.PERSONAL.PHYSICALADDRESS"]},
        ]

        out = _apply(spark, data, schema, mocked_cols)
        rows = out.orderBy("sapId").select("sapId", "address").collect()

        assert rows[0]["address"].addressLine1 == "REDACTED"
        assert rows[0]["address"].addressLine2 == "REDACTED"
        assert rows[0]["address"].townCity == "REDACTED"
        assert rows[0]["address"].county == "REDACTED"
        assert rows[0]["address"].postcode == "SW1A"

        assert rows[1]["address"].addressLine1 == "REDACTED"
        assert rows[1]["address"].addressLine2 == "REDACTED"
        assert rows[1]["address"].townCity == "REDACTED"
        assert rows[1]["address"].county == "REDACTED"
        assert rows[1]["address"].postcode == "BS1"

    def test__inspector__null_address_struct_is_preserved(self):
        """A null address struct value remains null after anonymisation."""
        spark = PytestSparkSessionUtil().get_spark_session()

        schema = StructType(
            [
                StructField("sapId", StringType(), True),
                StructField("address", _ADDRESS_SCHEMA, True),
            ]
        )
        data = [
            {"sapId": "S001", "address": None},
        ]
        mocked_cols = [
            {"column_name": "address", "classifications": ["MICROSOFT.PERSONAL.PHYSICALADDRESS"]},
        ]

        out = _apply(spark, data, schema, mocked_cols)
        row = out.select("address").collect()[0]

        assert row["address"] is None

    def test__inspector__null_pii_scalar_fields_remain_null(self):
        """Null values in firstName, lastName, email, and inspectorManager remain null."""
        spark = PytestSparkSessionUtil().get_spark_session()

        schema = StructType(
            [
                StructField("sapId", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
                StructField("email", StringType(), True),
                StructField("inspectorManager", StringType(), True),
            ]
        )
        data = [
            {
                "sapId": "S001",
                "firstName": None,
                "lastName": None,
                "email": None,
                "inspectorManager": None,
            },
        ]
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "lastName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
            {"column_name": "inspectorManager", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        ]

        out = _apply(spark, data, schema, mocked_cols)
        row = out.select("firstName", "lastName", "email", "inspectorManager").collect()[0]

        assert row["firstName"] is None
        assert row["lastName"] is None
        assert row["email"] is None
        assert row["inspectorManager"] is None

    def test__inspector__non_pii_fields_are_unchanged(self):
        """Non-PII fields (sapId, entraId, grade, isActive, fte, unit, service, group, title, sourceSystem) are not modified."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [
            {
                "sapId": "S001",
                "entraId": "aabbccdd-1234-5678-abcd-ef0123456789",
                "grade": "Grade 7",
                "isActive": "Y",
                "fte": "1.0",
                "unit": "Planning Inspectorate",
                "service": "National Infrastructure",
                "group": "Case Work",
                "title": "Inspector",
                "sourceSystem": "SAP_HR",
                "firstName": "John",
                "email": "john.doe@example.com",
            },
        ]
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        row = out.select("sapId", "entraId", "grade", "isActive", "fte", "unit", "service", "group", "title", "sourceSystem").collect()[0].asDict()

        assert row["sapId"] == "S001"
        assert row["entraId"] == "aabbccdd-1234-5678-abcd-ef0123456789"
        assert row["grade"] == "Grade 7"
        assert row["isActive"] == "Y"
        assert row["fte"] == "1.0"
        assert row["unit"] == "Planning Inspectorate"
        assert row["service"] == "National Infrastructure"
        assert row["group"] == "Case Work"
        assert row["title"] == "Inspector"
        assert row["sourceSystem"] == "SAP_HR"

    def test__inspector__all_pii_fields_anonymised_in_single_pass(self):
        """All PII fields are correctly anonymised together in a single apply_from_purview call."""
        spark = PytestSparkSessionUtil().get_spark_session()

        schema = StructType(
            [
                StructField("sapId", StringType(), True),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
                StructField("email", StringType(), True),
                StructField("inspectorManager", StringType(), True),
                StructField("grade", StringType(), True),
                StructField("address", _ADDRESS_SCHEMA, True),
            ]
        )
        data = [
            {
                "sapId": "S001",
                "firstName": "John",
                "lastName": "Doe",
                "email": "john.doe@example.com",
                "inspectorManager": "Jane Smith",
                "grade": "Grade 7",
                "address": {
                    "addressLine1": "15 Lambourne Road",
                    "addressLine2": None,
                    "townCity": "London",
                    "county": "Greater London",
                    "postcode": "SW1A 1AA",
                },
            },
        ]
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "lastName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
            {"column_name": "inspectorManager", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "address", "classifications": ["MICROSOFT.PERSONAL.PHYSICALADDRESS"]},
        ]

        out = _apply(spark, data, schema, mocked_cols)
        row = out.select("sapId", "firstName", "lastName", "email", "inspectorManager", "grade", "address").collect()[0]

        # PII fields anonymised
        assert row["firstName"] == "J***"
        assert row["lastName"] == "D**"
        assert row["email"] == "836f82db99121b3481011f16b49dfa5fbc714a0d1b1b9f784a1ebbbf5b39577f"
        assert row["inspectorManager"] == "J*** ****h"
        assert row["address"].addressLine1 == "REDACTED"
        assert row["address"].addressLine2 == "REDACTED"
        assert row["address"].townCity == "REDACTED"
        assert row["address"].county == "REDACTED"
        assert row["address"].postcode == "SW1A"

        # Non-PII unchanged
        assert row["sapId"] == "S001"
        assert row["grade"] == "Grade 7"

    def test__inspector__anonymisation_is_deterministic_across_runs(self):
        """Anonymisation produces the same output for the same input on repeated calls."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [{"sapId": "S001", "firstName": "John", "email": "john.doe@example.com"}]
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "email", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        ]

        out1 = _apply(spark, data, None, mocked_cols)
        out2 = _apply(spark, data, None, mocked_cols)

        row1 = out1.select("firstName", "email").collect()[0]
        row2 = out2.select("firstName", "email").collect()[0]

        assert row1["firstName"] == row2["firstName"]
        assert row1["email"] == row2["email"]

    def test__inspector__columns_not_in_purview_classifications_are_skipped(self):
        """Columns absent from Purview classifications are never touched, regardless of name."""
        spark = PytestSparkSessionUtil().get_spark_session()

        data = [{"sapId": "S001", "firstName": "John", "lastName": "Doe"}]
        # Only firstName is classified — lastName must not be anonymised
        mocked_cols = [
            {"column_name": "firstName", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        ]

        out = _apply(spark, data, None, mocked_cols)
        row = out.select("firstName", "lastName").collect()[0]

        assert row["firstName"] == "J***"
        assert row["lastName"] == "Doe"
