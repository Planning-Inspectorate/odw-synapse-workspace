from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.etl.etl_result import ETLResult, ETLSuccessResult, ETLFailResult
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from typing import Dict, Tuple
import traceback


class NsipDocumentHarmonisationProcess(HarmonisationProcess):
    """
    ETL process for harmonising NSIP Document data from service bus and Horizon sources.

    # Example usage via py_etl_orchestrator

    ```
    input_arguments = {
        "entity_stage_name": "nsip-document-harmonised",
        "debug": False
    }
    ```
    """

    SERVICE_BUS_TABLE = "odw_harmonised_db.sb_nsip_document"
    HORIZON_TABLE = "odw_standardised_db.document_meta_data"
    AIE_EXTRACTS_TABLE = "odw_harmonised_db.aie_document_data"
    OUTPUT_TABLE = "odw_harmonised_db.nsip_document"
    PRIMARY_KEY = "TEMP_PK"

    def __init__(self, spark: SparkSession, debug: bool = False):
        super().__init__(spark, debug)

    @classmethod
    def get_name(cls) -> str:
        return "nsip-document-harmonised"

    def load_data(self, **kwargs) -> Dict[str, DataFrame]:
        """
        Load data from:
        - Service bus harmonised table (sb_nsip_document)
        - Horizon standardised table (document_meta_data)
        - AIE extracts table (aie_document_data)
        """
        LoggingUtil().log_info(f"Loading service bus data from {self.SERVICE_BUS_TABLE}")
        service_bus_data = self._load_service_bus_data()

        LoggingUtil().log_info(f"Loading Horizon data from {self.HORIZON_TABLE} joined with {self.AIE_EXTRACTS_TABLE}")
        horizon_data = self._load_horizon_data()

        return {
            "service_bus_data": service_bus_data,
            "horizon_data": horizon_data,
        }

    def _load_service_bus_data(self) -> DataFrame:
        """
        Get data out of the service bus with additional fields needed for Horizon data.
        Computes a primary key: MD5(CONCAT(documentId, filename, version))
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(CONCAT(documentId, filename, version)) AS {self.PRIMARY_KEY}
                ,NSIPDocumentID
                ,documentId
                ,caseId
                ,caseRef
                ,documentReference
                ,version
                ,examinationRefNo
                ,filename
                ,originalFilename
                ,size
                ,mime
                ,documentURI
                ,publishedDocumentURI
                ,path
                ,virusCheckStatus
                ,fileMD5
                ,dateCreated
                ,lastModified
                ,caseType
                ,redactedStatus
                ,publishedStatus
                ,datePublished
                ,documentType
                ,securityClassification
                ,sourceSystem
                ,origin
                ,owner
                ,author
                ,authorWelsh
                ,representative
                ,description
                ,descriptionWelsh
                ,documentCaseStage
                ,filter1
                ,filter1Welsh
                ,filter2
                ,horizonFolderId
                ,transcriptId

                ,Migrated
                ,ODTSourceSystem
                ,SourceSystemID
                ,IngestionDate
                ,NULLIF(ValidTo, '') AS ValidTo
                ,'' as RowID
                ,IsActive
            FROM
                {self.SERVICE_BUS_TABLE}
        """)

    def _load_horizon_data(self) -> DataFrame:
        """
        Get data out of Horizon and match the SB schema (with additional fields and ensure data types match).
        LEFT JOINs to aie_document_data on dataid, version, and dataSize.
        Filters to latest expected_from date only.
        """
        return self.spark.sql(f"""
            SELECT DISTINCT
                MD5(
                    CONCAT(
                        COALESCE(Doc.dataId, ''),
                        COALESCE(Doc.name, ''),
                        COALESCE(CAST(Doc.version AS INTEGER), '')
                    )
                ) AS {self.PRIMARY_KEY}
                ,CAST(NULL AS LONG) AS NSIPDocumentID
                ,CAST(Doc.dataId AS INTEGER) as documentId
                ,CAST(Doc.casenodeid AS INTEGER) AS caseId
                ,Doc.caseReference AS caseRef
                ,Doc.documentReference
                ,CAST(Doc.version AS INTEGER) AS version
                ,Aie.examinationRefNo
                ,Doc.name as filename
                ,Doc.originalFilename as originalFilename
                ,CAST(Doc.dataSize AS INTEGER) AS size
                ,Aie.mime
                ,Aie.documentURI
                ,CAST(NULL AS String) AS publishedDocumentURI
                ,Aie.path
                ,Doc.virusCheckStatus
                ,Aie.fileMD5
                ,Cast(Doc.createDate as string) as dateCreated
                ,Doc.modifyDate as lastModified
                ,Doc.caseworkType as caseType
                ,CAST(NULL AS String) AS redactedStatus
                ,Doc.publishedStatus
                ,Cast(Doc.datePublished as string) as datePublished
                ,Doc.documentType
                ,Aie.securityClassification
                ,Doc.sourceSystem
                ,Aie.origin AS origin
                ,Aie.owner
                ,Doc.author
                ,Doc.authorWelsh
                ,Doc.representative
                ,Doc.documentDescription AS description
                ,Doc.documentDescriptionWelsh AS descriptionWelsh
                ,Doc.documentCaseStage
                ,Doc.filter1
                ,Doc.filter1Welsh
                ,Doc.filter2
                ,Doc.parentid AS horizonFolderId
                ,Null as transcriptId

                ,"0" as Migrated
                ,"Horizon" as ODTSourceSystem
                ,NULL AS SourceSystemID
                ,to_timestamp(Doc.expected_from) AS IngestionDate
                ,CAST(null as string) as ValidTo
                ,'' as RowID
                ,'Y' as IsActive
            FROM
                {self.HORIZON_TABLE} AS Doc
            LEFT JOIN {self.AIE_EXTRACTS_TABLE} AS Aie
            ON Doc.dataid = Aie.DocumentId
            AND Doc.version = Aie.version
            AND Doc.dataSize = Aie.size
            WHERE
                Doc.expected_from = (SELECT MAX(expected_from) FROM {self.HORIZON_TABLE})
        """)

    def process(self, **kwargs) -> Tuple[Dict[str, DataFrame], ETLResult]:
        """
        Combine service bus and Horizon data, compute IsActive/ValidTo/Migrated/RowID,
        deduplicate, and prepare for writing.
        """
        start_exec_time = datetime.now()

        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        service_bus_data: DataFrame = self.load_parameter("service_bus_data", source_data)
        horizon_data: DataFrame = self.load_parameter("horizon_data", source_data)

        # Step 1: Combine data - align Horizon columns to SB columns and union
        LoggingUtil().log_info(f"Combining data for {self.OUTPUT_TABLE}")
        horizon_data = horizon_data.select(service_bus_data.columns)
        results = service_bus_data.union(horizon_data)
        LoggingUtil().log_info(f"Combined data for {self.OUTPUT_TABLE}")

        # Step 2: Write intermediate table so we can use SQL window functions
        LoggingUtil().log_info(f"Writing intermediate {self.OUTPUT_TABLE}")
        results.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").partitionBy("IsActive").saveAsTable(self.OUTPUT_TABLE)
        LoggingUtil().log_info(f"Written intermediate {self.OUTPUT_TABLE}")

        # Step 3: Create calculations base view with window functions
        self.spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW vw_nsip_document_calculations_base
                AS
                SELECT
                    row_number() OVER(PARTITION BY {self.PRIMARY_KEY} ORDER BY IngestionDate DESC) AS ReverseOrderProcessed
                    ,row_number() OVER(ORDER BY IngestionDate asc, {self.PRIMARY_KEY} asc) AS NSIPDocumentID
                    ,{self.PRIMARY_KEY}
                    ,IngestionDate
                    ,ValidTo
                    ,'0' AS Migrated
                    ,CASE row_number() OVER(PARTITION BY {self.PRIMARY_KEY} ORDER BY IngestionDate DESC)
                        WHEN 1 THEN
                            'Y'
                        ELSE
                            'N'
                    END AS IsActive
                FROM
                    {self.OUTPUT_TABLE}
        """)

        # Step 4: Compute ValidTo and Migrated flags
        df_calcs = self.spark.sql(f"""
            SELECT
                CurrentRow.NSIPDocumentID
                ,CurrentRow.{self.PRIMARY_KEY}
                ,CurrentRow.IngestionDate
                ,COALESCE(NULLIF(CurrentRow.ValidTo,''), NextRow.IngestionDate) AS ValidTo
                ,CASE
                    WHEN Raw.{self.PRIMARY_KEY} IS NOT NULL THEN
                        "1"
                    ELSE
                        "0"
                END AS Migrated
                ,CurrentRow.IsActive
            FROM
                vw_nsip_document_calculations_base AS CurrentRow
                LEFT OUTER JOIN vw_nsip_document_calculations_base AS NextRow
                    ON CurrentRow.{self.PRIMARY_KEY} = NextRow.{self.PRIMARY_KEY}
                    AND CurrentRow.ReverseOrderProcessed - 1 = NextRow.ReverseOrderProcessed
                LEFT OUTER JOIN (SELECT DISTINCT MD5(CONCAT(documentId, filename, version)) AS {self.PRIMARY_KEY} FROM {self.SERVICE_BUS_TABLE}) AS Raw
                    ON CurrentRow.{self.PRIMARY_KEY} = Raw.{self.PRIMARY_KEY}
                ORDER BY CurrentRow.ReverseOrderProcessed
        """)

        # Step 5: Rename columns for the join
        df_calcs = df_calcs.withColumnRenamed(self.PRIMARY_KEY, f"temp_{self.PRIMARY_KEY}").withColumnRenamed("IngestionDate", "temp_IngestionDate")

        # Step 6: Build final results with RowID computation
        results = self.spark.sql(f"""
            SELECT DISTINCT
                {self.PRIMARY_KEY}
                ,NSIPDocumentID
                ,documentId
                ,caseId
                ,caseRef
                ,documentReference
                ,version
                ,examinationRefNo
                ,filename
                ,originalFilename
                ,size
                ,mime
                ,documentURI
                ,publishedDocumentURI
                ,path
                ,virusCheckStatus
                ,fileMD5
                ,dateCreated
                ,lastModified
                ,caseType
                ,redactedStatus
                ,publishedStatus
                ,datePublished
                ,documentType
                ,securityClassification
                ,sourceSystem
                ,origin
                ,owner
                ,author
                ,authorWelsh
                ,representative
                ,description
                ,descriptionWelsh
                ,documentCaseStage
                ,filter1
                ,filter1Welsh
                ,filter2
                ,horizonFolderId
                ,transcriptId

                ,Migrated
                ,ODTSourceSystem
                ,IngestionDate
                ,ValidTo
                ,MD5(CONCAT(
                        IFNULL(CAST(NSIPDocumentID AS bigint), '.')
                        ,IFNULL(CAST(documentId AS String), '.')
                        ,IFNULL(CAST(caseId AS integer), '.')
                        ,IFNULL(CAST(caseRef AS String), '.')
                        ,IFNULL(CAST(version AS integer), '.')
                        ,IFNULL(CAST(examinationRefNo AS integer), '.')
                        ,IFNULL(CAST(filename AS String), '.')
                        ,IFNULL(CAST(originalFilename AS String), '.')
                        ,IFNULL(CAST(size AS integer), '.')
                        ,IFNULL(CAST(mime AS String), '.')
                        ,IFNULL(CAST(documentURI AS String), '.')
                        ,IFNULL(CAST(publishedDocumentURI AS String), '.')
                        ,IFNULL(CAST(path AS String), '.')
                        ,IFNULL(CAST(virusCheckStatus AS String), '.')
                        ,IFNULL(CAST(fileMD5 AS String), '.')
                        ,IFNULL(CAST(dateCreated AS String), '.')
                        ,IFNULL(CAST(datePublished AS String), '.')
                        ,IFNULL(CAST(lastModified AS String), '.')
                        ,IFNULL(CAST(caseType AS String), '.')
                        ,IFNULL(CAST(redactedStatus AS String), '.')
                        ,IFNULL(CAST(documentType AS String), '.')
                        ,IFNULL(CAST(securityClassification AS String), '.')
                        ,IFNULL(CAST(sourceSystem AS String), '.')
                        ,IFNULL(CAST(origin AS String), '.')
                        ,IFNULL(CAST(owner AS String), '.')
                        ,IFNULL(CAST(author AS String), '.')
                        ,IFNULL(CAST(authorWelsh AS String), '.')
                        ,IFNULL(CAST(description AS String), '.')
                        ,IFNULL(CAST(descriptionWelsh AS String), '.')
                        ,IFNULL(CAST(documentCaseStage AS String), '.')
                        ,IFNULL(CAST(filter1 AS String), '.')
                        ,IFNULL(CAST(filter1Welsh AS String), '.')
                        ,IFNULL(CAST(filter2 AS String), '.')
                        ,IFNULL(CAST(horizonFolderId AS String), '.')
                        ,IFNULL(CAST(transcriptId AS String), '.')
                        ,IFNULL(CAST(publishedStatus AS String), '.')
                        ,IFNULL(CAST(Migrated AS String), '.')
                        ,IFNULL(CAST(ODTSourceSystem AS String), '.')
                        ,IFNULL(CAST(IngestionDate AS String), '.')
                        ,IFNULL(CAST(ValidTo AS String), '.')
                    )
                ) AS RowID
                ,IsActive
            FROM
                {self.OUTPUT_TABLE}
        """)

        # Step 7: Drop columns that will be replaced from calcs, then join
        columns = results.columns
        results = results.drop("NSIPDocumentID", "ValidTo", "Migrated", "IsActive")

        final_df = results.join(
            df_calcs,
            (df_calcs[f"temp_{self.PRIMARY_KEY}"] == results[self.PRIMARY_KEY])
            & (df_calcs["temp_IngestionDate"] == results["IngestionDate"]),
            ).select(columns)

        final_df = final_df.drop(self.PRIMARY_KEY).drop_duplicates()

        # Step 8: Deduplicate on key columns
        columns_to_consider = [
            "documentId", "filename", "version", "Migrated",
            "originalFilename", "ODTSourceSystem", "IngestionDate",
            "ValidTo", "IsActive",
        ]
        final_df = final_df.drop_duplicates(subset=columns_to_consider)

        insert_count = final_df.count()

        # Step 9: Write final result directly (overwrite the table)
        LoggingUtil().log_info(f"Writing finalised {self.OUTPUT_TABLE}")
        final_df.write.format("delta").mode("Overwrite").option("overwriteSchema", "true").partitionBy("IsActive").saveAsTable(self.OUTPUT_TABLE)
        LoggingUtil().log_info(f"Written finalised {self.OUTPUT_TABLE}")

        end_exec_time = datetime.now()

        # Return empty data_to_write since we wrote directly (matching notebook behaviour)
        return dict(), ETLSuccessResult(
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

    def run(self, **kwargs) -> ETLResult:
        """
        Run the full harmonisation ETL process for NSIP Document.

        This overrides the base ETLProcess.run() because the notebook writes the table
        directly within process() (intermediate writes are needed for SQL window functions),
        rather than via the standard write_data() flow.
        """
        etl_start_time = datetime.now()
        try:
            self.spark.catalog.refreshTable(self.HORIZON_TABLE)
        except Exception:
            LoggingUtil().log_info(f"Could not refresh table {self.HORIZON_TABLE}, continuing")

        try:
            source_data_map = self.load_data(**kwargs)
            LoggingUtil().log_info(f"Loaded source data: {list(source_data_map.keys())}")
            data_to_write, etl_result = self.process(source_data=source_data_map, **kwargs)
        except Exception as e:
            end_time = datetime.now()
            failure_result = ETLFailResult(
                metadata=ETLResult.ETLResultMetadata(
                    start_execution_time=etl_start_time,
                    end_execution_time=end_time,
                    exception=str(e),
                    exception_trace=traceback.format_exc(),
                    table_name=self.OUTPUT_TABLE,
                    activity_type=self.__class__.__name__,
                    duration_seconds=(end_time - etl_start_time).total_seconds(),
                    insert_count=0,
                    update_count=0,
                    delete_count=0,
                )
            )
            LoggingUtil().log_error(failure_result)
            return failure_result

        if isinstance(etl_result, ETLFailResult):
            LoggingUtil().log_error(etl_result)
            return etl_result

        LoggingUtil().log_info(etl_result)
        return etl_result




