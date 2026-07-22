from odw.core.etl.historical_anonymisation.historical_anonymisation_process import (
    HistoricalAnonymisationProcess,
)
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from typing import Dict


class EntraIDHistoricalAnonymisationProcess(HistoricalAnonymisationProcess):
    @classmethod
    def get_name(cls):
        return "EntraId Historical Data Anonymisation Process"

    def process(self, **kwargs):
        # Extract the "value" field from the raw data
        source_data: Dict[str, DataFrame] = self.load_parameter("source_data", kwargs)
        raw_data: DataFrame = self.load_parameter("raw_data", source_data)
        raw_data_exploded = raw_data.withColumn(
            "exploded", F.explode(F.col("value"))
        ).select("exploded.*")
        source_data["raw_data"] = raw_data_exploded
        kwargs["source_data"] = source_data
        return super().process(**kwargs)
