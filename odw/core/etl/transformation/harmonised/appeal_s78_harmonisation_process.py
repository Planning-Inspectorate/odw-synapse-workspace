from odw.core.etl.transformation.harmonised.harmonisation_process import (
    HarmonisationProcess,
)
from pyspark.sql import DataFrame


class AppealS78HarmonisationProcess(HarmonisationProcess):
    SERVICE_BUS_TABLE = "sb_appeal_s78"
    HORIZON_TABLE = "horizon_appeal_s78"
    HAS_TABLE = "appeal_has"
    GROUP_RESOLVER = "GroupResolver"
    OUTPUT_TABLE = "appeal_s78"

    @classmethod
    def get_name(cls) -> str:
        return "Appeal S78 Harmonisation Process"

    def _load_service_bus_data(self) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._load_service_bus_data() has not been implemented yet."
        )

    def _load_horizon_data(self) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._load_horizon_data() has not been implemented yet."
        )

    def _load_appeal_has_data(self) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._load_appeal_has_data() has not been implemented yet."
        )

    def load_data(self, **kwargs):
        raise NotImplementedError(
            "AppealS78HarmonisationProcess.load_data() has not been implemented yet."
        )

    def _clean_service_bus_data(self, raw_service_bus_data: DataFrame):
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._clean_service_bus_data() has not been implemented yet."
        )

    def _clean_horizon_data(self, raw_horizon_data: DataFrame) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._clean_horizon_data() has not been implemented yet."
        )

    def _clean_appeal_has_data(self, raw_has_data: DataFrame) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._clean_appeal_has_data() has not been implemented yet."
        )

    def _aggregate_data(
        self, clean_service_bus_data: DataFrame, clean_horizon_data: DataFrame
    ) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._aggregate_data() has not been implemented yet."
        )

    def _clean_aggregate_data(self, aggregate_data: DataFrame) -> DataFrame:
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._clean_aggregate_data() has not been implemented yet."
        )

    def _apply_slowly_changing_dimensions(
        self,
        clean_aggregate_data: DataFrame,
        appeal_has_data: DataFrame,
        service_bus_data: DataFrame,
    ) -> DataFrame:
        # Should include all logic related to creating the scd and scd2_final variables
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._apply_slowly_changing_dimensions() has not been implemented yet."
        )

    def _generate_new_group_resolver_data(self, scd_data: DataFrame):
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._generate_new_group_resolver_data() has not been implemented yet."
        )

    def _generate_new_has_data(self, scd_data: DataFrame, appeal_has_data: DataFrame):
        raise NotImplementedError(
            "AppealS78HarmonisationProcess._generate_new_has_data() has not been implemented yet."
        )

    def process(self, **kwargs):
        # Shouldn't need to do the column deduplication logic in the original notebook since it doesn't actually seem to work
        raise NotImplementedError(
            "AppealS78HarmonisationProcess.process() has not been implemented yet."
        )
