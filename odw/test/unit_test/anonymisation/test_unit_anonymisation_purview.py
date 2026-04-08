from unittest import mock
import re
from datetime import date
from odw.core.anonymisation import AnonymisationEngine
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.assertion import assert_dataframes_equal


def test_engine_apply_from_purview_with_mocked_classifications():
    spark = PytestSparkSessionUtil().get_spark_session()

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
            "NINumber": "BB111111B",
            "BirthDate": "1985-05-05",
            "AnnualSalary": 60000,
        },
    ]
    df = spark.createDataFrame(data)

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

    assert rows[0]["full_name"] == "J*** **e"
    assert rows[1]["full_name"] == "J*** ****h"

    assert rows[0]["emailAddress"] == "j******e@example.com"
    assert rows[1]["emailAddress"] == "j********h@example.com"

    for age in (rows[0]["Age"], rows[1]["Age"]):
        assert 18 <= age <= 70

    ni_re = re.compile(r"^[A-Z]{2}\d{6}[A-D]$")
    for nin in (rows[0]["NINumber"], rows[1]["NINumber"]):
        assert isinstance(nin, str)
        assert ni_re.match(nin)
        assert nin not in {"AA000000A", "BB111111B"}

    start = date(1955, 1, 1)
    end = date(2005, 12, 31)
    for dob in (rows[0]["BirthDate"], rows[1]["BirthDate"]):
        assert start <= dob <= end

    for sal in (rows[0]["AnnualSalary"], rows[1]["AnnualSalary"]):
        assert isinstance(sal, int)
        assert 20000 <= sal <= 100000

    assert out.count() == df.count()
    assert set(out.columns) == set(df.columns)


def test_engine_apply_from_purview_returns_input_unchanged_when_no_classifications_found():
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"EmployeeID": "E1", "full_name": "John Doe", "emailAddress": "john.doe@example.com"},
        {"EmployeeID": "E2", "full_name": "Jane Smith", "emailAddress": "jane.smith@example.com"},
    ]
    df = spark.createDataFrame(data)

    engine = AnonymisationEngine()

    with mock.patch(
        "odw.core.anonymisation.engine.fetch_purview_classifications_by_qualified_name",
        return_value=[],
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

    assert_dataframes_equal(df, out)


def test_engine_apply_from_purview_honours_classification_allowlist():
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"EmployeeID": "E1", "full_name": "John Doe", "emailAddress": "john.doe@example.com", "Age": 34},
    ]
    df = spark.createDataFrame(data)

    mocked_cols = [
        {"column_name": "full_name", "classifications": ["MICROSOFT.PERSONAL.NAME"]},
        {"column_name": "emailAddress", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
        {"column_name": "Age", "classifications": ["Person's Age"]},
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
            classification_allowlist=["MICROSOFT.PERSONAL.EMAIL"],
        )

    row = out.collect()[0].asDict()

    assert row["emailAddress"] == "j******e@example.com"
    assert row["full_name"] == "John Doe"
    assert row["Age"] == 34


def test_engine_apply_from_purview_ignores_unsupported_classifications():
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"EmployeeID": "E1", "free_text": "Some value"},
    ]
    df = spark.createDataFrame(data)

    mocked_cols = [
        {"column_name": "free_text", "classifications": ["UNSUPPORTED.CLASSIFICATION.TYPE"]},
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

    assert_dataframes_equal(df, out)


def test_engine_apply_from_purview_matches_normalised_column_names():
    spark = PytestSparkSessionUtil().get_spark_session()

    data = [
        {"EmployeeID": "E1", "emailAddress": "john.doe@example.com"},
    ]
    df = spark.createDataFrame(data)

    mocked_cols = [
        {"column_name": "Email Address", "classifications": ["MICROSOFT.PERSONAL.EMAIL"]},
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

    row = out.collect()[0].asDict()
    assert row["emailAddress"] == "j******e@example.com"
