from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from odw.core.anonymisation import AnonymisationEngine
from odw.core.anonymisation.base import (
    _seed_col,
    random_int_from_seed,
    random_date_from_seed,
    mask_fullname_initial_lastletter_udf,
    mask_email_preserve_domain_udf,
)
import mock
import re


def test_apply_from_purview__mocked_fetch_and_spark_df():
    spark = SparkSession.builder.master("local[1]").appName("anon-purview-test").getOrCreate()
    try:
        df = spark.createDataFrame(
            [
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
        )

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

        # Build expected deterministic output for comparable columns
        cols = ["EmployeeID", "full_name", "emailAddress", "Age", "BirthDate", "AnnualSalary"]

        seed = _seed_col(df)
        expected = (
            df.withColumn("full_name", mask_fullname_initial_lastletter_udf(F.col("full_name")))
            .withColumn("emailAddress", mask_email_preserve_domain_udf(F.col("emailAddress")))
            .withColumn("Age", random_int_from_seed(seed, 18, 70).cast("int"))
            .withColumn("BirthDate", random_date_from_seed(seed))
            .withColumn("AnnualSalary", random_int_from_seed(seed, 20000, 100000).cast("int"))
        )

        actual_rows = out.select(*cols).orderBy("EmployeeID").collect()
        expected_rows = expected.select(*cols).orderBy("EmployeeID").collect()
        assert actual_rows == expected_rows

        # NI number is intentionally randomised; validate format only
        ni_re = re.compile(r"^[A-Z]{2}\d{6}[A-D]$")
        for row in out.select("NINumber").orderBy("EmployeeID").collect():
            nin = row[0]
            assert isinstance(nin, str) and ni_re.match(nin)
    finally:
        spark.stop()
