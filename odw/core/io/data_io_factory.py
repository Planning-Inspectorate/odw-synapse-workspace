from odw.core.io.data_io import DataIO
from odw.core.exceptions import DuplicateDataIONameException, DataIONameNotFoundException
from odw.core.io.data_io import DataIO
from odw.core.io.synapse_file_data_io import SynapseFileDataIO
from odw.core.io.synapse_table_data_io import SynapseTableDataIO
from odw.core.io.azure_blob_data_io import AzureBlobDataIO
from typing import Set, List, Dict, Type
import json


class DataIOFactory():
    DATA_IO_CLASSES: Set[Type[DataIO]] = {
        SynapseFileDataIO,
        SynapseTableDataIO,
        AzureBlobDataIO
    }

    @classmethod
    def _validate_data_io_classes(cls):
        name_map: Dict[str, List[Type[DataIO]]] = dict()
        for data_io_class in cls.DATA_IO_CLASSES:
            type_name = data_io_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(data_io_class)
            else:
                name_map[type_name] = [data_io_class]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateDataIONameException(
                f"The following DataIO implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, data_io_name: str) -> Type[DataIO]:
        data_io_map = cls._validate_data_io_classes()
        if data_io_name not in data_io_map:
            raise DataIONameNotFoundException(
                f"No DataIO class could be found for dataio name '{data_io_name}'"
            )
        return data_io_map[data_io_name]
