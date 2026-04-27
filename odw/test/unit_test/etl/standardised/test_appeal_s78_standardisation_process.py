import mock
import pytest
from pyspark.sql.types import ArrayType, StringType, StructField, StructType, TimestampType
from odw.core.etl.transformation.standardised.appeal_s78_standardisation_process import AppealS78StandardisationProcess
from odw.test.util.session_util import PytestSparkSessionUtil
from odw.test.util.test_case import SparkTestCase
from odw.test.util.assertion import assert_dataframes_equal


class TestAppealS78StandardisationProcess(SparkTestCase):
    @pytest.fixture()
    def mock_data_loaders(self):
        yield

    def test__appeal_s78_standardisation_process__load_horizoncases_s78(self):
        """
        h_1row AS (
            SELECT * FROM (
                SELECT h.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY h.caseuniqueid
                        ORDER BY h.expected_from DESC,
                                h.modified_datetime DESC,
                                h.ingested_datetime DESC,
                                h.file_id DESC
                    ) rn
                FROM h h
            ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_cases_specialisms(self):
        def generate_case_specialisms_row(**overrides):
            base = {
                "ingested_datetime": None,
                "expected_from": None,
                "expected_to": None,
                "casereference": "",
                "lastmodified": None,
                "casespecialism": None,
                "input_file": "somefile.parquet",
                "ingested_by_process_name": "some_process",
                "modified_datetime": None,
                "modified_by_process_name": "some_process",
                "entity_name": "cases_specialisms",
                "file_id": None
            }
            return base | overrides

        spark = PytestSparkSessionUtil().get_spark_session()
        raw_data = spark.createDataFrame(
            (
                generate_case_specialisms_row(file_id="1", casereference="refA"),
                generate_case_specialisms_row(file_id="2", casereference="refB", casespecialism=""),
                generate_case_specialisms_row(file_id="2", casereference="refC", casespecialism="Some Specialism"),
                generate_case_specialisms_row(file_id="3", casereference="refD", casespecialism="Some Specialism"),
                generate_case_specialisms_row(file_id="4", casereference="refE", casespecialism="Another Specialism")
                
            ),
            schema=StructType(
                [
                    StructField("ingested_datetime", TimestampType(), True),
                    StructField("expected_from", TimestampType(), True),
                    StructField("expected_to", TimestampType(), True),
                    StructField("casereference", StringType(), True),
                    StructField("lastmodified", StringType(), True),
                    StructField("casespecialism", StringType(), True),
                    StructField("input_file", StringType(), True),
                    StructField("ingested_by_process_name", StringType(), True),
                    StructField("modified_datetime", TimestampType(), True),
                    StructField("modified_by_process_name", StringType(), True),
                    StructField("entity_name", StringType(), True),
                    StructField("file_id", StringType(), True),
                ]
            ),
        )
        # write here
        expected_data = spark.createDataFrame(
            (
                {"casereference": "refC", "casespecialism": "Some Specialism"},
                {"casereference": "refD", "casespecialism": "Some Specialism"},
                {"casereference": "refE", "casespecialism": "Another Specialism"},
            ),
            schema=StructType([StructField('casereference', StringType(), True), StructField('casespecialism', StringType(), False)])
        )
        assert_dataframes_equal(expected_data, )

        """
        cs_agg AS (
        SELECT
            casereference,
            concat_ws(', ', sort_array(collect_set(trim(casespecialism)))) AS casespecialism
        FROM cs
        WHERE casespecialism IS NOT NULL AND trim(casespecialism) <> ''
        GROUP BY casereference
        """

    def test__appeal_s78_standardisation_process__load_vw_case_dates(self):
        """
        cd_1row AS (
            SELECT * FROM (
                SELECT x.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY x.casenodeid
                        ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                    ) rn
                FROM cd x
            ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_casedocumentdatesdates(self):
        """
        cdd_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cdd x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_casesitestrings(self):
        """
        css_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM css x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_typeofprocedure(self):
        """
        add_procedure AS (
        SELECT a.*, tp.proccode AS caseProcedure
        FROM add_aad a
        LEFT JOIN tp ON a.procedureType = tp.name
        )
        """

    def test__appeal_s78_standardisation_process__load_vw_addadditionaldata(self):
        """
        aad_old_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM aad_old x
        ) t WHERE rn = 1
        ),
        """

    def test__appeal_s78_standardisation_process__load_vw_additionalfields(self):
        """
        af_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM af x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_advert_attributes(self):
        """
        haa_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.caseuniqueid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM haa x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_TypeOfLevel(self):
        """
        add_typeoflevel AS (
        SELECT b.*, ctl.name AS allocationLevel, ctl.band AS allocationBand
        FROM base b
        LEFT JOIN ctl ON b.level_code = ctl.name
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_specialist_case_dates(self):
        """
        scd_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM scd x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_PlanningAppStrings(self):
        """
        pas_1row AS (
        SELECT * FROM (
            SELECT p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.casenodeid
                    ORDER BY p.expected_from DESC, p.modified_datetime DESC, p.ingested_datetime DESC, p.file_id DESC
                ) rn
            FROM pas p
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_PlanningAppDates(self):
        """
        pad_1row AS (
        SELECT * FROM (
            SELECT p.*,
                ROW_NUMBER() OVER (
                    PARTITION BY p.casenodeid
                    ORDER BY p.expected_from DESC, p.modified_datetime DESC, p.ingested_datetime DESC, p.file_id DESC
                ) rn
            FROM pad p
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_BIS_LeadCase(self):
        """
        lc_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM lc x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_CaseStrings(self):
        """
        cs2_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cs2 x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_case_info(self):
        """
        ci_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM ci x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_case_dates(self):
        """
        cdh_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM cdh x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_appeals_additional_data(self):
        """
        ad_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.appealrefnumber
                    ORDER BY x.expected_from DESC, x.modified_datetime DESC, x.ingested_datetime DESC, x.file_id DESC
                ) rn
            FROM aad x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_appeal_grounds(self):
        """
        hag_agg AS (
        SELECT
            g.casenodeid,
            sort_array(
            collect_list(
                named_struct(
                'appealGroundLetter', g.appealgroundletter,
                'groundForAppealStartDate', g.groundforappealstartdate
                )
            )
            ) AS enforcementAppealGroundsDetails
        FROM hag g
        GROUP BY g.casenodeid
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_notice_dates(self):
        """
        hnd_1row AS (
        SELECT * FROM (
            SELECT n.*,
                ROW_NUMBER() OVER (
                    PARTITION BY n.casenodeid
                    ORDER BY n.expected_from DESC,
                            n.setrownumber DESC,
                            n.ingested_datetime DESC,
                            n.file_id DESC
                ) rn
            FROM hnd n
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_horizon_application_made_under_section(self):
        """
        hmu_1row AS (
        SELECT * FROM (
            SELECT x.*,
                ROW_NUMBER() OVER (
                    PARTITION BY x.casenodeid
                    ORDER BY x.expected_from DESC,
                            x.modified_datetime DESC,
                            x.ingested_datetime DESC,
                            x.file_id DESC
                ) rn
            FROM hmu x
        ) t WHERE rn = 1
        )
        """

    def test__appeal_s78_standardisation_process__load_data(self, mock_data_loaders):
        expected_output_keys = {
            "odw_standardised_db.horizoncases_s78",
            "odw_standardised_db.cases_specialisms",
            "odw_standardised_db.vw_case_dates",
            "odw_standardised_db.casedocumentdatesdates",
            "odw_standardised_db.casesitestrings",
            "odw_standardised_db.typeofprocedure",
            "odw_standardised_db.vw_addadditionaldata",
            "odw_standardised_db.vw_additionalfields",
            "odw_standardised_db.horizon_advert_attributes",
            "odw_standardised_db.TypeOfLevel",
            "odw_standardised_db.horizon_specialist_case_dates",
            "odw_standardised_db.PlanningAppStrings",
            "odw_standardised_db.PlanningAppDates",
            "odw_standardised_db.BIS_LeadCase",
            "odw_standardised_db.CaseStrings",
            "odw_standardised_db.horizon_case_info",
            "odw_standardised_db.horizon_case_dates",
            "odw_standardised_db.horizon_appeals_additional_data",
            "odw_standardised_db.horizon_appeal_grounds",
            "odw_standardised_db.horizon_notice_dates",
            "odw_standardised_db.horizon_application_made_under_section",
        }
