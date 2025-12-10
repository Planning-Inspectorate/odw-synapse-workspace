import pytest
import mock
import re
from datetime import date
from odw.core.anonymisation import AnonymisationEngine

try:
    from pyspark.sql import SparkSession
except ModuleNotFoundError:
    pytest.skip("PySpark not installed; skipping anonymisation Purview tests", allow_module_level=True)


def test_engine_apply_from_purview_with_mocked_fetch():
    spark = SparkSession.builder.master("local[1]").appName("anon-purview-test").getOrCreate()
    try:
        # Test data
        data = [
            {
                "EmployeeID": "E1",
                "full_name": "John Doe",
                "emailAddress": "john.doe@example.com",
                "Age": 34,
                "NINumber": "AA000000A",
                "BirthDate": "1990-01-01",
                "AnnualSalary": 50000,
            },
            {
                "EmployeeID": "E2",
                "full_name": "Jane Smith",
                "emailAddress": "jane.smith@example.com",
                "Age": 45,
                "NINumber": "AA000000A",
                "BirthDate": "1985-05-05",
                "AnnualSalary": 60000,
            },
        ]
        df = spark.createDataFrame(data)

        # What Purview would have returned for an ADLS Gen2 asset
        mocked_cols = [
            {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
            {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
            {"column_name": "Age", "classifications": ["Person's Age"]},
            {"column_name": "NINumber", "classifications": ["NI Number"]},
            {"column_name": "BirthDate", "classifications": ["Birth Date"]},
            {"column_name": "AnnualSalary", "classifications": ["Annual Salary"]},
        ]

        engine = AnonymisationEngine()

        with mock.patch(
            "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
            return_value=mocked_cols,
        ):
            out = engine.apply_from_purview(
                df,
                purview_name="dummy-pview",
                tenant_id="dummy-tenant",
                client_id="dummy-client",
                client_secret="dummy-secret",
                asset_type_name="azure_datalake_gen2_resource_set",
                asset_qualified_name="https://dummy.dfs.core.windows.net/container/path/asset.csv",
            )

        rows = out.select("full_name", "emailAddress", "Age", "NINumber", "BirthDate", "AnnualSalary").collect()

        # Name masking: first letter of first name, last letter of last name
        assert rows[0][0] == "J*** **e"
        assert rows[1][0] == "J*** ****h"

        # Email masking: mask local part and preserve domain
        assert rows[0][1] == "j******e@example.com"
        assert rows[1][1] == "j********h@example.com"

        # Age remapped into anonymised range [18, 70]
        for age in (rows[0][2], rows[1][2]):
            assert 18 <= age <= 70

        # NI number format: two letters, six digits, final letter A-D
        ni_re = re.compile(r"^[A-Z]{2}\d{6}[A-D]$")
        for nin in (rows[0][3], rows[1][3]):
            assert isinstance(nin, str) and ni_re.match(nin)

        # Birth date within expected anonymised range
        start = date(1955, 1, 1)
        end = date(2005, 12, 31)
        for dob in (rows[0][4], rows[1][4]):
            assert start <= dob <= end

        # Salary within anonymised range
        for sal in (rows[0][5], rows[1][5]):
            assert isinstance(sal, int) and 20000 <= sal <= 100000
    finally:
        spark.stop()
