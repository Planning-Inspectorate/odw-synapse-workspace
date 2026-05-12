import mock
import pytest
import odw.test.util.mock.import_mock_notebook_utils  # noqa: F401
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from odw.core.etl.transformation.standardised.listed_building_standardisation_process import ListedBuildingStandardisationProcess
from odw.test.integration_test.etl.etl_test_case import ETLTestCase
from odw.test.util.assertion import assert_dataframes_equal
from odw.test.util.session_util import PytestSparkSessionUtil

pytestmark = pytest.mark.xfail(reason="Standardisation logic not implemented yet")

LISTED_BUILDING_OUTPUT_TABLE = "listed_building"
LISTED_BUILDING_OUTLINE_OUTPUT_TABLE = "listed_building_outline"


def _listed_building_entity_schema():
    return StructType(
        [
            StructField("dataset", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("name", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("listed-building-grade", StringType(), True),
        ]
    )


def _listed_building_outline_entity_schema():
    return StructType(
        [
            StructField("address", StringType(), True),
            StructField("address-text", StringType(), True),
            StructField("dataset", StringType(), True),
            StructField("document-url", StringType(), True),
            StructField("documentation-url", StringType(), True),
            StructField("end-date", StringType(), True),
            StructField("entity", StringType(), True),
            StructField("entry-date", StringType(), True),
            StructField("geometry", StringType(), True),
            StructField("listed-building", StringType(), True),
            StructField("name", StringType(), True),
            StructField("notes", StringType(), True),
            StructField("organisation-entity", StringType(), True),
            StructField("point", StringType(), True),
            StructField("prefix", StringType(), True),
            StructField("reference", StringType(), True),
            StructField("start-date", StringType(), True),
            StructField("typology", StringType(), True),
        ]
    )


def _raw_listed_building_schema():
    return StructType(
        [
            StructField(
                "entities",
                ArrayType(_listed_building_entity_schema()),
                True,
            )
        ]
    )


def _raw_listed_building_outline_schema():
    return StructType(
        [
            StructField(
                "entities",
                ArrayType(_listed_building_outline_entity_schema()),
                True,
            )
        ]
    )


def _empty_listed_building_raw_df(spark):
    return spark.createDataFrame([], schema=_raw_listed_building_schema())


def _empty_listed_building_outline_raw_df(spark):
    return spark.createDataFrame([], schema=_raw_listed_building_outline_schema())


def _listed_building_raw_df(spark):
    return spark.createDataFrame(
        [
            (
                [
                    {
                        "dataset": "listed-building",
                        "end-date": None,
                        "entity": "1001",
                        "entry-date": "2024-01-01",
                        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
                        "name": "Building One",
                        "organisation-entity": "org-1",
                        "point": "POINT(1 1)",
                        "prefix": "listed-building",
                        "reference": "LB-001",
                        "start-date": "2020-01-01",
                        "typology": "grade-ii",
                        "documentation-url": "https://example.com/lb-001",
                        "listed-building-grade": "II",
                    },
                    {
                        "dataset": "listed-building",
                        "end-date": "2025-01-01",
                        "entity": "1002",
                        "entry-date": "2024-02-01",
                        "geometry": None,
                        "name": "Building Two",
                        "organisation-entity": "org-2",
                        "point": None,
                        "prefix": "listed-building",
                        "reference": "LB-002",
                        "start-date": "2021-01-01",
                        "typology": "grade-i",
                        "documentation-url": None,
                        "listed-building-grade": "I",
                    },
                ],
            )
        ],
        schema=_raw_listed_building_schema(),
    )


def _listed_building_outline_raw_df(spark):
    return spark.createDataFrame(
        [
            (
                [
                    {
                        "address": "1 High Street",
                        "address-text": "1 High Street, Town",
                        "dataset": "listed-building-outline",
                        "document-url": "https://example.com/doc-1",
                        "documentation-url": "https://example.com/outline-1",
                        "end-date": None,
                        "entity": "2001",
                        "entry-date": "2024-03-01",
                        "geometry": "POLYGON((4 4,5 5,6 6,4 4))",
                        "listed-building": "1001",
                        "name": "Outline One",
                        "notes": "Some notes",
                        "organisation-entity": "org-1",
                        "point": "POINT(4 4)",
                        "prefix": "listed-building-outline",
                        "reference": "LBO-001",
                        "start-date": "2022-01-01",
                        "typology": "outline",
                    },
                    {
                        "address": None,
                        "address-text": None,
                        "dataset": "listed-building-outline",
                        "document-url": None,
                        "documentation-url": None,
                        "end-date": "2025-06-01",
                        "entity": "2002",
                        "entry-date": "2024-04-01",
                        "geometry": None,
                        "listed-building": "1002",
                        "name": "Outline Two",
                        "notes": None,
                        "organisation-entity": "org-2",
                        "point": None,
                        "prefix": "listed-building-outline",
                        "reference": "LBO-002",
                        "start-date": "2023-01-01",
                        "typology": "outline",
                    },
                ],
            )
        ],
        schema=_raw_listed_building_outline_schema(),
    )


def _listed_building_multirow_raw_df(spark):
    return spark.createDataFrame(
        [
            (
                [
                    {
                        "dataset": "listed-building",
                        "end-date": None,
                        "entity": "1001",
                        "entry-date": "2024-01-01",
                        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
                        "name": "Building One",
                        "organisation-entity": "org-1",
                        "point": "POINT(1 1)",
                        "prefix": "listed-building",
                        "reference": "LB-001",
                        "start-date": "2020-01-01",
                        "typology": "grade-ii",
                        "documentation-url": "https://example.com/lb-001",
                        "listed-building-grade": "II",
                    },
                ],
            ),
            (
                [
                    {
                        "dataset": "listed-building",
                        "end-date": None,
                        "entity": "1002",
                        "entry-date": "2024-02-01",
                        "geometry": None,
                        "name": "Building Two",
                        "organisation-entity": "org-2",
                        "point": None,
                        "prefix": "listed-building",
                        "reference": "LB-002",
                        "start-date": "2021-01-01",
                        "typology": "grade-i",
                        "documentation-url": None,
                        "listed-building-grade": "I",
                    },
                    {
                        "dataset": "listed-building",
                        "end-date": None,
                        "entity": "1003",
                        "entry-date": "2024-03-01",
                        "geometry": "POLYGON((7 7,8 8,9 9,7 7))",
                        "name": "Building Three",
                        "organisation-entity": "org-3",
                        "point": "POINT(7 7)",
                        "prefix": "listed-building",
                        "reference": "LB-003",
                        "start-date": "2022-01-01",
                        "typology": "grade-ii-star",
                        "documentation-url": "https://example.com/lb-003",
                        "listed-building-grade": "II*",
                    },
                ],
            ),
        ],
        schema=_raw_listed_building_schema(),
    )


def _listed_building_outline_multirow_raw_df(spark):
    return spark.createDataFrame(
        [
            (
                [
                    {
                        "address": "1 High Street",
                        "address-text": "1 High Street, Town",
                        "dataset": "listed-building-outline",
                        "document-url": "https://example.com/doc-1",
                        "documentation-url": "https://example.com/outline-1",
                        "end-date": None,
                        "entity": "2001",
                        "entry-date": "2024-03-01",
                        "geometry": "POLYGON((4 4,5 5,6 6,4 4))",
                        "listed-building": "1001",
                        "name": "Outline One",
                        "notes": "Some notes",
                        "organisation-entity": "org-1",
                        "point": "POINT(4 4)",
                        "prefix": "listed-building-outline",
                        "reference": "LBO-001",
                        "start-date": "2022-01-01",
                        "typology": "outline",
                    },
                ],
            ),
            (
                [
                    {
                        "address": None,
                        "address-text": None,
                        "dataset": "listed-building-outline",
                        "document-url": None,
                        "documentation-url": None,
                        "end-date": "2025-06-01",
                        "entity": "2002",
                        "entry-date": "2024-04-01",
                        "geometry": None,
                        "listed-building": "1002",
                        "name": "Outline Two",
                        "notes": None,
                        "organisation-entity": "org-2",
                        "point": None,
                        "prefix": "listed-building-outline",
                        "reference": "LBO-002",
                        "start-date": "2023-01-01",
                        "typology": "outline",
                    },
                    {
                        "address": "3 Low Street",
                        "address-text": "3 Low Street, Village",
                        "dataset": "listed-building-outline",
                        "document-url": "https://example.com/doc-3",
                        "documentation-url": "https://example.com/outline-3",
                        "end-date": None,
                        "entity": "2003",
                        "entry-date": "2024-05-01",
                        "geometry": "POLYGON((10 10,11 11,12 12,10 10))",
                        "listed-building": "1003",
                        "name": "Outline Three",
                        "notes": "Another outline",
                        "organisation-entity": "org-3",
                        "point": "POINT(10 10)",
                        "prefix": "listed-building-outline",
                        "reference": "LBO-003",
                        "start-date": "2024-01-01",
                        "typology": "outline",
                    },
                ],
            ),
        ],
        schema=_raw_listed_building_outline_schema(),
    )


def _listed_building_empty_entities_raw_df(spark):
    return spark.createDataFrame([([],)], schema=_raw_listed_building_schema())


def _listed_building_outline_empty_entities_raw_df(spark):
    return spark.createDataFrame([([],)], schema=_raw_listed_building_outline_schema())


def _duplicate_listed_building_raw_df(spark):
    entity = {
        "dataset": "listed-building",
        "end-date": None,
        "entity": "1001",
        "entry-date": "2024-01-01",
        "geometry": "POLYGON((1 1,2 2,3 3,1 1))",
        "name": "Building One",
        "organisation-entity": "org-1",
        "point": "POINT(1 1)",
        "prefix": "listed-building",
        "reference": "LB-001",
        "start-date": "2020-01-01",
        "typology": "grade-ii",
        "documentation-url": "https://example.com/lb-001",
        "listed-building-grade": "II",
    }
    return spark.createDataFrame([([entity, entity],)], schema=_raw_listed_building_schema())


def _source_data(spark, listed_building_raw_df=None, listed_building_outline_raw_df=None):
    return {
        "listed_building_data": listed_building_raw_df if listed_building_raw_df is not None else _empty_listed_building_raw_df(spark),
        "listed_building_outline_data": listed_building_outline_raw_df if listed_building_outline_raw_df is not None else _empty_listed_building_outline_raw_df(spark),
    }


class TestListedBuildingStandardisationProcess(ETLTestCase):
    def test__listed_building_standardisation_process__run__writes_both_outputs_end_to_end_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _listed_building_raw_df(spark), _listed_building_outline_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        assert listed_building_df.columns == [field.name for field in _listed_building_entity_schema()]
        assert listed_building_outline_df.columns == [field.name for field in _listed_building_outline_entity_schema()]

        expected_lb_df = spark.createDataFrame(
            [
                ("listed-building", None, "1001", "2024-01-01", "POLYGON((1 1,2 2,3 3,1 1))", "Building One", "org-1", "POINT(1 1)", "listed-building", "LB-001", "2020-01-01", "grade-ii", "https://example.com/lb-001", "II"),
                ("listed-building", "2025-01-01", "1002", "2024-02-01", None, "Building Two", "org-2", None, "listed-building", "LB-002", "2021-01-01", "grade-i", None, "I"),
            ],
            schema=_listed_building_entity_schema(),
        )
        expected_lbo_df = spark.createDataFrame(
            [
                ("1 High Street", "1 High Street, Town", "listed-building-outline", "https://example.com/doc-1", "https://example.com/outline-1", None, "2001", "2024-03-01", "POLYGON((4 4,5 5,6 6,4 4))", "1001", "Outline One", "Some notes", "org-1", "POINT(4 4)", "listed-building-outline", "LBO-001", "2022-01-01", "outline"),
                (None, None, "listed-building-outline", None, None, "2025-06-01", "2002", "2024-04-01", None, "1002", "Outline Two", None, "org-2", None, "listed-building-outline", "LBO-002", "2023-01-01", "outline"),
            ],
            schema=_listed_building_outline_entity_schema(),
        )
        assert_dataframes_equal(listed_building_df, expected_lb_df)
        assert_dataframes_equal(listed_building_outline_df, expected_lbo_df)

    def test__listed_building_standardisation_process__run__preserves_flattened_values_and_nulls_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _listed_building_raw_df(spark), _listed_building_outline_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        lb_row = listed_building_df.where("entity = '1002'").select("end-date", "geometry", "point", "documentation-url").collect()[0]
        lbo_row = (
            listed_building_outline_df.where("entity = '2002'")
            .select("address", "address-text", "document-url", "documentation-url", "geometry", "notes", "point")
            .collect()[0]
        )

        assert lb_row["end-date"] == "2025-01-01"
        assert lb_row["geometry"] is None
        assert lb_row["point"] is None
        assert lb_row["documentation-url"] is None

        assert lbo_row["address"] is None
        assert lbo_row["address-text"] is None
        assert lbo_row["document-url"] is None
        assert lbo_row["documentation-url"] is None
        assert lbo_row["geometry"] is None
        assert lbo_row["notes"] is None
        assert lbo_row["point"] is None

    def test__listed_building_standardisation_process__run__explodes_entities_from_multiple_raw_rows(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _listed_building_multirow_raw_df(spark), _listed_building_outline_multirow_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        assert listed_building_df.count() == 3
        assert listed_building_outline_df.count() == 3

    def test__listed_building_standardisation_process__run__empty_entities_arrays_return_empty_outputs(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _listed_building_empty_entities_raw_df(spark), _listed_building_outline_empty_entities_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        assert listed_building_df.count() == 0
        assert listed_building_outline_df.count() == 0

    def test__listed_building_standardisation_process__run__empty_input_dataframes_return_empty_outputs(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark)
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        assert listed_building_df.count() == 0
        assert listed_building_outline_df.count() == 0

    def test__listed_building_standardisation_process__run__one_source_empty_still_writes_the_other_output(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _listed_building_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]
        listed_building_outline_df = data_to_write[LISTED_BUILDING_OUTLINE_OUTPUT_TABLE]["data"]

        assert listed_building_df.count() == 2
        assert listed_building_outline_df.count() == 0

    def test__listed_building_standardisation_process__run__preserves_duplicate_entities_like_legacy(self):
        spark = PytestSparkSessionUtil().get_spark_session()
        source_data = _source_data(spark, _duplicate_listed_building_raw_df(spark))
        inst = ListedBuildingStandardisationProcess(spark)

        with (
            mock.patch.object(inst, "load_data", return_value=source_data),
            mock.patch.object(inst, "write_data") as mock_write,
        ):
            inst.run()

        data_to_write = mock_write.call_args[0][0]
        listed_building_df = data_to_write[LISTED_BUILDING_OUTPUT_TABLE]["data"]

        assert listed_building_df.count() == 2
        assert listed_building_df.where("entity = '1001'").count() == 2
