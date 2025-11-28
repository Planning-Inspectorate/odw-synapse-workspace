from odw.core.io.parser.data_file_parser import DataFileParser
from odw.core.exceptions import DuplicateDataFileParserNameException, DataFileParserNameNotFoundException
from typing import Dict, Set, Type
import json


class DataFileParserFactory():
    PARSERS: Set[DataFileParser] = {

    }

    @classmethod
    def _validate_parser_classes(cls):
        name_map: Dict[str, Set[Type[DataFileParser]]] = dict()
        for parser_class in cls.PARSERS:
            type_name = parser_class.get_name()
            if type_name in name_map:
                name_map[type_name].append(parser_class)
            else:
                name_map[type_name] = [parser_class]
        invalid_types = {
            k: v
            for k, v in name_map.items()
            if len(v) > 1
        }
        if invalid_types:
            raise DuplicateDataFileParserNameException(
                f"The following DataFileParser implementation classes had duplicate names: {json.dumps(invalid_types, indent=4)}"
            )
        return {
            k: v[0]
            for k, v in name_map.items()
        }

    @classmethod
    def get(cls, parser_map: str) -> Type[DataFileParser]:
        parser_map = cls._validate_parser_classes()
        if parser_map not in parser_map:
            raise DataFileParserNameNotFoundException(
                f"No DataFileParser class could be found for DataFileParser name '{parser_map}'"
            )
        return parser_map[parser_map]
