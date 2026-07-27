from odw.core.etl.transformation.transformation_process import TransformationProcess
from odw.core.anonymisation.config import load_config, AnonymisationConfig
from odw.core.anonymisation.engine import AnonymisationEngine
from odw.core.util.logging_util import LoggingUtil
from odw.core.util.util import Util
from pyspark.sql import DataFrame


class StandardisationProcess(TransformationProcess):
    def try_anonymise_data(
        self,
        data: DataFrame,
        file_name: str,
        source_folder: str,
        entity_name_for_seed: str = "",
        ignore_exceptions: bool = False,
    ):
        """
        Anonymise the given dataframe if the process is running in a non-live environment. Else just return the data as-is
        """
        _anon_enabled = Util.is_non_production_environment()
        LoggingUtil().log_info(
            f"anonymisation_gate: environment={Util.get_environment()} enabled={_anon_enabled} file={file_name}"
        )
        if _anon_enabled:
            try:
                anon_config = AnonymisationConfig()
                try:
                    policy_path = Util.get_path_to_file(
                        "odw-config/anonymisation/policy.yaml"
                    )
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
                        seed_column=anon_config.get_seed_column(entity_name_for_seed),
                    )
                )
                LoggingUtil().log_info(
                    f"Applying anonymisation to Horizon file: {file_name}"
                )
                return engine.apply_from_purview(
                    data, file_name=file_name, source_folder=source_folder
                )
            except Exception as e:
                LoggingUtil().log_error(
                    f"Anonymisation failed for {file_name}: {str(e)}"
                )
                if ignore_exceptions:
                    return data
                raise
        return data
