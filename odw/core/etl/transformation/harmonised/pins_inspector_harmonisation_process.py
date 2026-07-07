from odw.core.etl.transformation.harmonised.harmonisation_process import (
    HarmonisationProcess,
)
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult
from odw.core.anonymisation.config import load_config, AnonymisationConfig
from odw.core.anonymisation.engine import (
    AnonymisationEngine,
    fetch_purview_classifications_by_qualified_name,
)
from pyspark.sql import DataFrame, Window as W
from pyspark.sql import functions as F
from datetime import datetime
from typing import Dict, Tuple
import os


class PinsInspectorHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for building the harmonised PINS Inspector dataset from SAP HR sources.

    # Example usage via py_etl_executor

    ```
    input_arguments = {
        "etl_process_name": "PINS Inspector Harmonisation Process",
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

    @classmethod
    def get_name(cls) -> str:
        return "PINS Inspector Harmonisation Process"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        return {
            "entraid": self.spark.table(self.ENTRAID_TABLE),
            "specialisms": self.spark.table(self.INSPECTOR_SPECIALISMS_TABLE),
            "address": self.spark.table(self.INSPECTOR_ADDRESS_TABLE),
            "live_dim": self.spark.table(self.LIVE_DIM_TABLE),
            "hist_hr": self.spark.table(self.HIST_SAP_HR_TABLE),
        }

    def _dedup_specialisms(self, specialisms: DataFrame) -> DataFrame:
        """Latest specialism per (StaffNumber, QualificationName) among active records, collected into an array."""
        w = W.partitionBy("StaffNumber", "QualificationName").orderBy(
            F.col("ValidFrom").desc()
        )
        return (
            specialisms.filter(F.col("Current") == 1)
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .groupBy("StaffNumber")
            .agg(
                F.collect_list(
                    F.struct(
                        F.col("QualificationName").alias("name"),
                        F.col("Proficien").alias("proficiency"),
                        F.col("ValidFrom").alias("validFrom"),
                    )
                ).alias("specialisms")
            )
            .withColumnRenamed("StaffNumber", "sapId")
        )

    def _pad_entraid(self, entraid: DataFrame) -> DataFrame:
        """Pad SAP IDs shorter than 8 chars with '00' prefix; keep only active records."""
        return entraid.filter(F.col("isActive") == "Y").select(
            F.col("id"),
            F.when(
                F.length(F.col("employeeId")) < 8,
                F.concat(F.lit("00"), F.col("employeeId")),
            )
            .otherwise(F.col("employeeId"))
            .alias("employeeId"),
        )

    def _dedup_address(self, address: DataFrame) -> DataFrame:
        """One address row per inspector, keeping the most recent by IngestionDate."""
        w = W.partitionBy("StaffNumber").orderBy(F.col("IngestionDate").desc())
        return (
            address.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .withColumnRenamed("2ndAddressLine", "addressLine2")
        )

    def _latest_hr(self, hist_hr: DataFrame) -> DataFrame:
        """Most recent HR record per person."""
        w = W.partitionBy("PersNo").orderBy(F.col("ingestionDate").desc())
        return (
            hist_hr.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
            .select(
                "PersNo",
                "Position1",
                "FTE",
                "PersonnelArea",
                "PersonnelSubArea",
                "OrganizationalUnit",
                "NameofManagerOM",
                "SourceSystemID",
            )
        )

    def _join_inspectors(
        self,
        live_dim: DataFrame,
        spec_dedup: DataFrame,
        entraid_padded: DataFrame,
        addr_dedup: DataFrame,
        hr_latest: DataFrame,
    ) -> DataFrame:
        """Left-join all prepared sources onto the active live_dim rows."""
        di = live_dim.filter(~F.col("active_status").isin("NON-ACTIVE")).alias("di")
        ispec = spec_dedup.alias("ispec")
        eid = entraid_padded.alias("eid")
        addr = addr_dedup.alias("addr")
        hr = hr_latest.alias("hr")

        return (
            di.join(
                ispec, F.col("di.pins_staff_number") == F.col("ispec.sapId"), "left"
            )
            .join(eid, F.col("eid.employeeId") == F.col("di.pins_staff_number"), "left")
            .join(
                addr, F.col("addr.StaffNumber") == F.col("di.pins_staff_number"), "left"
            )
            .join(hr, F.col("hr.PersNo") == F.col("di.pins_staff_number"), "left")
            .select(
                F.col("eid.id").alias("entraId"),
                F.col("di.pins_staff_number").alias("sapId"),
                F.col("di.pins_email_address").alias("email"),
                F.col("di.given_names").alias("firstName"),
                F.col("di.family_name").alias("lastName"),
                F.col("di.date_in").alias("validFrom"),
                F.col("di.grade"),
                F.col("di.isActive"),
                F.coalesce(F.col("ispec.specialisms"), F.array()).alias("specialisms"),
                F.struct(
                    F.col("addr.StreetandHouseNumber").alias("addressLine1"),
                    F.col("addr.addressLine2"),
                    F.col("addr.City").alias("townCity"),
                    F.col("addr.District").alias("county"),
                    F.col("addr.PostalCode").alias("postcode"),
                ).alias("address"),
                F.col("hr.FTE").alias("fte"),
                F.col("hr.PersonnelArea").alias("unit"),
                F.col("hr.PersonnelSubArea").alias("service"),
                F.col("hr.OrganizationalUnit").alias("group"),
                F.col("hr.NameofManagerOM").alias("inspectorManager"),
                F.col("hr.Position1").alias("title"),
                F.col("hr.SourceSystemID").alias("sourceSystem"),
            )
        )

    def _apply_anonymisation(self, df: DataFrame) -> DataFrame:
        if not Util.is_non_production_environment():
            LoggingUtil().log_info(
                f"Environment: {Util.get_environment()} - Skipping anonymisation (production)"
            )
            return df

        env_name = Util.get_environment()
        LoggingUtil().log_info(
            f"Environment: {env_name} - Applying anonymisation to pins_inspector"
        )

        anon_config = AnonymisationConfig()
        try:
            policy_path = Util.get_path_to_file("odw-config/anonymisation/policy.yaml")
            policy_text = (
                self.spark.read.text(policy_path, wholetext=True).first().value
            )
            anon_config = load_config(text=policy_text)
        except Exception as config_err:
            LoggingUtil().log_info(
                f"Could not load anonymisation policy, using defaults: {config_err}"
            )

        engine = AnonymisationEngine(
            config=AnonymisationConfig(
                classification_allowlist=anon_config.classification_allowlist,
                seed_column="sapId",
            )
        )

        qualified_name = f"mssql://pins-synw-odw-{env_name.lower()}-uks-ondemand.sql.azuresynapse.net/odw_harmonised_db/dbo/pins_inspector"

        try:
            from notebookutils import mssparkutils

            client_id = mssparkutils.credentials.getSecretWithLS(
                "ls_kv", "synapse-graph-client-id"
            )
            client_secret = mssparkutils.credentials.getSecretWithLS(
                "ls_kv", "synapse-graph-client-secret"
            )
        except Exception:
            client_id = os.getenv(
                "ODW_CLIENT_ID", "5750ab9b-597c-4b0d-b0f0-f4ef94e91fc0"
            )
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

        spec_dedup = self._dedup_specialisms(specialisms)
        entraid_padded = self._pad_entraid(entraid)
        addr_dedup = self._dedup_address(address)
        hr_latest = self._latest_hr(hist_hr)

        final_df = self._join_inspectors(
            live_dim, spec_dedup, entraid_padded, addr_dedup, hr_latest
        )

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
        output_db, output_table_name = self.OUTPUT_TABLE.split(".", 1)
        data_to_write = {
            self.OUTPUT_TABLE: {
                "data": final_df,
                "storage_kind": "ADLSG2-Table",
                "database_name": output_db,
                "table_name": output_table_name,
                "storage_endpoint": Util.get_storage_account(),
                "container_name": "odw-harmonised",
                "blob_path": output_table_name,
                "file_format": "delta",
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
