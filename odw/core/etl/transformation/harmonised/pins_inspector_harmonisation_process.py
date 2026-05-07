from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.anonymisation.config import load_config, AnonymisationConfig
from odw.core.anonymisation.engine import AnonymisationEngine, fetch_purview_classifications_by_qualified_name
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple
import os


class PinsInspectorHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for building the harmonised PINS Inspector dataset from SAP HR sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "pins-inspector-harmonised",
        "debug": False
    }
    ```
    """

    ENTRAID_TABLE = "odw_harmonised_db.entraid"
    INSPECTOR_SPECIALISMS_TABLE = "odw_harmonised_db.sap_hr_inspector_specialisms"
    INSPECTOR_ADDRESS_TABLE = "odw_harmonised_db.sap_hr_inspector_address"
    LIVE_DIM_TABLE = "odw_harmonised_db.live_dim_inspector"
    HIST_SAP_HR_TABLE = "odw_harmonised_db.hist_sap_hr"
    OUTPUT_TABLE = "odw_harmonised_db.pins_inspector"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "pins-inspector-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        return {
            "entraid": self.spark.table(self.ENTRAID_TABLE),
            "specialisms": self.spark.table(self.INSPECTOR_SPECIALISMS_TABLE),
            "address": self.spark.table(self.INSPECTOR_ADDRESS_TABLE),
            "live_dim": self.spark.table(self.LIVE_DIM_TABLE),
            "hist_hr": self.spark.table(self.HIST_SAP_HR_TABLE),
        }

    def _apply_anonymisation(self, df: DataFrame) -> DataFrame:
        if not Util.is_non_production_environment():
            LoggingUtil().log_info(f"Environment: {Util.get_environment()} - Skipping anonymisation (production)")
            return df

        env_name = Util.get_environment()
        LoggingUtil().log_info(f"Environment: {env_name} - Applying anonymisation to pins_inspector")

        anon_config = AnonymisationConfig()
        try:
            policy_path = Util.get_path_to_file("odw-config/anonymisation/policy.yaml")
            policy_text = self.spark.read.text(policy_path, wholetext=True).first().value
            anon_config = load_config(text=policy_text)
        except Exception as config_err:
            LoggingUtil().log_info(f"Could not load anonymisation policy, using defaults: {config_err}")

        engine = AnonymisationEngine(
            config=AnonymisationConfig(
                classification_allowlist=anon_config.classification_allowlist,
                seed_column="sapId",
            )
        )

        qualified_name = (
            f"mssql://pins-synw-odw-{env_name.lower()}-uks-ondemand.sql.azuresynapse.net"
            "/odw_harmonised_db/dbo/pins_inspector"
        )

        try:
            from notebookutils import mssparkutils

            client_id = mssparkutils.credentials.getSecretWithLS("ls_kv", "synapse-graph-client-id")
            client_secret = mssparkutils.credentials.getSecretWithLS("ls_kv", "synapse-graph-client-secret")
        except Exception:
            client_id = os.getenv("ODW_CLIENT_ID", "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0")
            client_secret = "AZURE_IDENTITY"

        cols = fetch_purview_classifications_by_qualified_name(
            purview_name="pins-pview",
            tenant_id="5878df98-6f88-48ab-9322-998ce557088d",
            client_id=client_id,
            client_secret=client_secret,
            asset_type_name="azure_synapse_serverless_sql_table",
            asset_qualified_name=qualified_name,
        )

        LoggingUtil().log_info(f"Found {len(cols)} classified columns in Purview")
        return engine.apply(df, cols)

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        start_exec_time = datetime.now()
        source_data = self.load_parameter("source_data", kwargs)

        entraid: DataFrame = self.load_parameter("entraid", source_data)
        specialisms: DataFrame = self.load_parameter("specialisms", source_data)
        address: DataFrame = self.load_parameter("address", source_data)
        live_dim: DataFrame = self.load_parameter("live_dim", source_data)
        hist_hr: DataFrame = self.load_parameter("hist_hr", source_data)

        # Register as uniquely prefixed temp views to avoid cross-test conflicts
        entraid.createOrReplaceTempView("_pi_entraid")
        specialisms.createOrReplaceTempView("_pi_specialisms")
        address.createOrReplaceTempView("_pi_address")
        live_dim.createOrReplaceTempView("_pi_live_dim")
        hist_hr.createOrReplaceTempView("_pi_hist_hr")

        final_df = self.spark.sql("""
            WITH
            -- Deduplicate specialisms: latest per (StaffNumber, QualificationName) among active records
            insp_spec_dedup AS (
                SELECT
                    StaffNumber AS sapId,
                    collect_list(
                        named_struct(
                            'name',        QualificationName,
                            'proficiency', Proficien,
                            'validFrom',   ValidFrom
                        )
                    ) AS specialisms
                FROM (
                    SELECT StaffNumber, QualificationName, Proficien, ValidFrom
                    FROM (
                        SELECT *,
                            ROW_NUMBER() OVER (
                                PARTITION BY StaffNumber, QualificationName
                                ORDER BY ValidFrom DESC
                            ) AS rn
                        FROM _pi_specialisms
                        WHERE Current = 1
                    ) ranked_spec
                    WHERE rn = 1
                )
                GROUP BY StaffNumber
            ),
            -- Pad SAP IDs shorter than 8 chars with '00' prefix for consistent matching
            entraid_padded AS (
                SELECT
                    id,
                    CASE
                        WHEN LEN(employeeId) < 8 THEN CONCAT('00', employeeId)
                        ELSE employeeId
                    END AS employeeId
                FROM _pi_entraid
                WHERE isActive = 'Y'
            ),
            -- Deduplicate address records: one row per inspector (latest ingestion)
            addr_dedup AS (
                SELECT *
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY StaffNumber
                            ORDER BY IngestionDate DESC
                        ) AS rn
                    FROM _pi_address
                ) ranked_addr
                WHERE rn = 1
            ),
            -- Latest HR record per person
            hr_latest AS (
                SELECT
                    PersNo,
                    Position1,
                    FTE,
                    PersonnelArea,
                    PersonnelSubArea,
                    OrganizationalUnit,
                    NameofManagerOM,
                    SourceSystemID
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (
                            PARTITION BY PersNo
                            ORDER BY ingestionDate DESC
                        ) AS rn
                    FROM _pi_hist_hr
                ) ranked
                WHERE rn = 1
            )
            SELECT
                eid.id                                        AS entraId,
                di.pins_staff_number                          AS sapId,
                di.pins_email_address                         AS email,
                di.given_names                                AS firstName,
                di.family_name                                AS lastName,
                di.date_in                                    AS validFrom,
                di.grade,
                di.isActive,
                COALESCE(ispec.specialisms, ARRAY())          AS specialisms,
                named_struct(
                    'addressLine1', addr.StreetandHouseNumber,
                    'addressLine2', addr.`2ndAddressLine`,
                    'townCity',     addr.City,
                    'county',       addr.District,
                    'postcode',     addr.PostalCode
                )                                             AS address,
                hr.FTE                                        AS fte,
                hr.PersonnelArea                              AS unit,
                hr.PersonnelSubArea                           AS service,
                hr.OrganizationalUnit                         AS `group`,
                hr.NameofManagerOM                            AS inspectorManager,
                hr.Position1                                  AS title,
                hr.SourceSystemID                             AS sourceSystem
            FROM _pi_live_dim di
            LEFT JOIN insp_spec_dedup ispec ON di.pins_staff_number = ispec.sapId
            LEFT JOIN entraid_padded    eid  ON eid.employeeId       = di.pins_staff_number
            LEFT JOIN addr_dedup        addr ON addr.StaffNumber     = di.pins_staff_number
            LEFT JOIN hr_latest         hr   ON hr.PersNo            = di.pins_staff_number
            WHERE di.active_status NOT IN ('NON-ACTIVE')
        """)

        # Append ISO-8601 time suffix to date-only validFrom values
        final_df = final_df.withColumn(
            "validFrom",
            F.when(
                F.col("validFrom").isNotNull(),
                F.concat(F.col("validFrom"), F.lit("T00:00:00.000Z")),
            ).otherwise(F.col("validFrom")),
        )

        final_df = self._apply_anonymisation(final_df)

        insert_count = final_df.count()
        LoggingUtil().log_info(f"Harmonised PINS Inspector row count: {insert_count}")

        end_exec_time = datetime.now()
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": "odw_harmonised_db",
                "table_name": "pins_inspector",
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": "pins_inspector",
                "file_format": "parquet",
                "write_mode": "overwrite",
                "write_options": {"overwriteSchema": "true"},
            }
        }
        return data_to_write, ETLSuccessResult(
            metadata=ETLResult.ETLResultMetadata(
                start_execution_time=start_exec_time,
                end_execution_time=end_exec_time,
                table_name=self.OUTPUT_TABLE,
                insert_count=insert_count,
                update_count=0,
                delete_count=0,
                activity_type=self.__class__.__name__,
                duration_seconds=(end_exec_time - start_exec_time).total_seconds(),
            )
        )
