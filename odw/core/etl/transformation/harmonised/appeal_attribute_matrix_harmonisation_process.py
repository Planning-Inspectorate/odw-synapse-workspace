from odw.core.etl.transformation.harmonised.harmonsation_process import HarmonisationProcess
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from typing import Any


class AppealAttributeMatrixHarmonisationProcess(HarmonisationProcess):
    OUTPUT_TABLE = "ref_appeal_attribute_matrix"

    def __init__(self, spark):
        super().__init__(spark)
        self.spark = spark
        self.logger = LoggingUtil(self.__class__.__name__)

    def load_data(self) -> dict[str, Any]:
        _ = Util.get_storage_account()
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.load_data() has not been implemented yet."
        )

    def process(self, source_data: dict[str, Any]):
        raise NotImplementedError(
            "AppealAttributeMatrixHarmonisationProcess.process() has not been implemented yet."
        )