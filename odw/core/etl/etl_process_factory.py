from odw.core.etl.etl_process import ETLProcess
from odw.core.exceptions import DuplicateETLProcessNameException, ETLProcessNameNotFoundException
from odw.core.etl.transformation.standardised.standardisation_process import StandardisationProcess
from odw.core.etl.transformation.standardised.service_bus_standardisation_process import ServiceBusStandardisationProcess
from typing import Dict, List, Set, Type
import json


class ETLProcessFactory():
    ETL_PROCESSES: Set[Type[ETLProcess]] = {
        StandardisationProcess,
        ServiceBusStandardisationProcess
    }

    @classmethod
    def _validate_etl_process_classes(cls):
        name_map: Dict[str, List[Type[ETLProcess]]] = dict()
        for etl_process_class in cls.ETL_PROCESSES:
            type_name = etl_process_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(etl_process_class)
            else:
                name_map[type_name] = [etl_process_class]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateETLProcessNameException(
                f"The following ETLProcess implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, etl_process_name: str) -> Type[ETLProcess]:
        etl_process_map = cls._validate_etl_process_classes()
        if etl_process_name not in etl_process_map:
            raise ETLProcessNameNotFoundException(
                f"No ETLProcess class could be found for ETLProcess name '{etl_process_name}'"
            )
        return etl_process_map[etl_process_name]
