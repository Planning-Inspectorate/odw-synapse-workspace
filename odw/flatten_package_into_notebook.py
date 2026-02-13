from typing import List, Dict, Any, Set, Union
import os
import ast
from ast2json import ast2json
from dataclasses import dataclass
from graphlib import TopologicalSorter
import argparse
import re


"""
This is a utility script to flatten the contents of the ODW package into a single script, so that it can be copy/pasted
into a notebook in Synapse. This allows you to bypass needing to reupload the package to the pool, which is a slow process.

This works by building a direct acyclic graph (DAG) of the package, and forms the combined file using the topological order of the
files. All relative imports within the package are commented out, since the dependencies are already included nearer the top of the
combined file

# Example usage
python3 odw/flatten_package_into_notebook.py  # Flattens everything in the package. This is usually not needed
python3 odw/flatten_package_into_notebook.py -r "odw/core/etl/etl_process_factory.py"  # Flattens just the ETL code (and its dependencies)

This will produce a file called odw/flatten_package_into_notebook__output.py , which you can copy into Synapse
"""
class AttributeNotFoundException(Exception):
    pass


@dataclass
class JsonPropertyIteratorResult:
    parent_collection: Union[Dict[str, Any], List[Any]]
    attribute: str
    value: Union[Dict[str, Any], List[Any], Any]
    remaining_attribute: str


class JsonPropertyIterator:
    """
    Class to iterate through the properties of a json object through dot notation
    """

    def __init__(self, dictionary: Dict[str, Any], attribute: str):
        self.parent_attribute_collection: Union[Dict[str, Any], List[Any]] = None
        self.attribute_collection: Union[Dict[str, Any], List[Any]] = dictionary
        self.attribute_split = [x for x in attribute.split(".") if x]
        if not self.attribute_split:
            raise ValueError(f"There is no attribute to evaluate")
        self.last_evaluated_attribute = None

    def __iter__(self):
        return self

    def __next__(self):
        """
        :return: The most recently-accessed collection
        :return: The most recently-accessed property
        :return: The value associated with the most recent property in the most recent collection
        :return: The remaining attribute string

        """
        if not self.attribute_split:
            # print()
            raise StopIteration
        # Imagine everything below is wrapped in a while loop as long as self.attribute_split has value
        self.last_evaluated_attribute = self.attribute_split.pop(0)
        if isinstance(self.attribute_collection, list):
            if not self.last_evaluated_attribute.isdigit():
                raise AttributeNotFoundException(f"Trying to access sub property '{self.last_evaluated_attribute}' on a list collection")
            self.last_evaluated_attribute = int(self.last_evaluated_attribute)
            if not (0 <= self.last_evaluated_attribute < len(self.attribute_collection)):
                raise AttributeNotFoundException(f"List index out of range for subproperty '{self.last_evaluated_attribute}' in list collection")
            self.parent_attribute_collection = self.attribute_collection
            self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
        elif isinstance(self.attribute_collection, dict):
            if self.last_evaluated_attribute not in self.attribute_collection:
                if not self.attribute_split:
                    raise AttributeNotFoundException(
                        f"Sub attribute '{self.last_evaluated_attribute}' not in dictionary collection {self.attribute_collection}"
                    )
                # Special case where the key contains a period
                while self.attribute_split:
                    next_attribute = self.attribute_split.pop(0)
                    self.last_evaluated_attribute += f".{next_attribute}"
                    if self.last_evaluated_attribute in self.attribute_collection:
                        self.parent_attribute_collection = self.attribute_collection
                        self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
            else:
                self.parent_attribute_collection = self.attribute_collection
                self.attribute_collection = self.attribute_collection[self.last_evaluated_attribute]
        else:
            if len(self.attribute_split) > 0:
                raise ValueError(
                    f"Trying to access a leaf property of a collection, but the remaining sub properties still need to be expanded: {self.attribute_split}"
                )
        # print(f"Last evaluated: '{self.last_evaluated_attribute}'")
        return JsonPropertyIteratorResult(
            self.parent_attribute_collection, self.last_evaluated_attribute, self.attribute_collection, ".".join(self.attribute_split)
        )


class AttributeExtractor:
    @classmethod
    def get_by_attribute(cls, dictionary: Dict[str, Any], attribute: str) -> Any:
        """
        Access json attributes by a dot-notation string.
        e.g `get_by_attribute({"a": {"b": 2}}, "a.b") -> 2`

        :param dictionary: The json artifact to extract the attribute from
        :param attribute: The attribute to extract, in dot-notation
        :return: The attribute value
        :raises: An exception is raised if the given inputs are invalid
        """
        property_details = [x for x in JsonPropertyIterator(dictionary, attribute)]
        return property_details.pop().value

    @classmethod
    def get_all_attributes(cls, artifact_json: Dict[str, Any]) -> Set[str]:
        """
        Return all attributes from the given json in dot notation

        :param artifact_json: The json object to get attributes for
        :return: The attributes of the json object, in dot-notation. e.g: some.attribute.of.the.json.object

        e.g
        ```
        my_json = {
            "some": 1,
            "attributes": 2,
            "list_type": [
                "a"
            ],
            "dict_type": {
                "b": 3,
                "c": 4
            }
        }
        # Then
        get_all_attributes(my_json)
        # Will return
        {
            "some",
            "attributes",
            "list_type.0",
            "dict_type.b",
            "dict_type.c"
        }
        ```
        """
        return set(cls._extract_dict_attributes(artifact_json, current_level="").keys())

    @classmethod
    def _extract_list_attributes(cls, target_list: List[Any], current_level: str) -> Dict[str, None]:
        """
        Extract attribute names from the given list in dot-notation

        :param target_list: The list to extract attributes from
        :param current_level: String used to prefix any new attributes with. Used to identify the depth of the analysis so far
        :return: Extracted attributes as a dict of the form <attribute: None>. (A dict is used to preserve the order of the attributes)

        e.g.
        ```
        my_list = ["a", {"list": 1, "of": 2, "attributes": 3}]
        _extract_list_attributes(my_list, "some_prefix")
        # Returns
        {
            "some_prefix.0": None,
            "some_prefix.1.list": None,
            "some_prefix.1.of": None,
            "some_prefix.1.attributes": None
        }
        ```
        """
        current_level_prefix = f"{current_level}." if current_level else ""
        dict_keys = dict()
        for i, val in enumerate(target_list):
            new_level = f"{current_level_prefix}{i}"
            if isinstance(val, dict):
                dict_keys = dict(dict_keys, **cls._extract_dict_attributes(val, new_level))
            elif isinstance(val, list):
                dict_keys = dict(dict_keys, **cls._extract_list_attributes(val, new_level))
            else:
                dict_keys[new_level] = None
        return dict_keys

    @classmethod
    def _extract_dict_attributes(cls, target_dict: Dict[str, Any], current_level: str = "") -> Dict[str, None]:
        """
        Extract attribute names from the given dictionary

        :param target_dict: The dictionary to extract attributes from
        :param current_level: String used to prefix any new attributes with. Used to identify the depth of the analysis so far
        :return: Extracted attributes as a dict of the form <attribute: None>. (A dict is used to preserve the order of the attributes)

        e.g.
        ```
        my_dict = {"group": 1, "of": 2, "attributes": 3, "a_list": {"a", "b", "c}, "nested": {"a": 1, "b"; 2}}
        _extract_list_attributes(my_dict, "some_prefix")
        # Returns
        {
            "some_prefix": None,
            "some_prefix.group": None,
            "some_prefix.of": None,
            "some_prefix.attributes": None,
            "some_prefix.a_list.0": None,
            "some_prefix.a_list.1": None,
            "some_prefix.a_list.2": None,
            "some_prefix.nested.a": None,
            "some_prefix.nested.b": None
        }
        ```
        """
        current_level_prefix = f"{current_level}." if current_level else ""
        dict_keys = dict()
        for key, val in target_dict.items():
            new_level = f"{current_level_prefix}{key}"
            if isinstance(val, dict):
                dict_keys = dict(dict_keys, **cls._extract_dict_attributes(val, new_level))
            elif isinstance(val, list):
                dict_keys = dict(dict_keys, **cls._extract_list_attributes(val, new_level))
            else:
                dict_keys[new_level] = None
        return dict_keys


def get_all_odw_package_content():
    module_content_to_exclude = {"__init__.py", "__pycache__"}
    python_modules_to_load = sorted(
        [
            os.path.join(path, name)
            for path, subdirs, files in os.walk(os.path.join("odw", "core"))
            for name in files
            if not any(x in os.path.join(path, name) for x in module_content_to_exclude) and name.endswith(".py")
        ]
    )
    python_modules_package_map = {x: x.replace(".py", "").replace(os.sep, ".") for x in python_modules_to_load}
    module_content_map = {import_path: open(module, "r").read() for module, import_path in python_modules_package_map.items()}
    return module_content_map


def extract_imports_from_file(module_content: str):
    abstract_syntax_tree = ast.parse(module_content)
    abstract_syntax_tree_json = ast2json(abstract_syntax_tree)
    ast_attributes = AttributeExtractor().get_all_attributes(abstract_syntax_tree_json)
    import_attr_types = {"Import", "ImportFrom"}
    import_type_attributes = {x for x in ast_attributes if AttributeExtractor.get_by_attribute(abstract_syntax_tree_json, x) in import_attr_types}
    import_from_values = {
        AttributeExtractor.get_by_attribute(abstract_syntax_tree_json, x.replace("_type", "module"))
        for x in import_type_attributes
        if AttributeExtractor.get_by_attribute(abstract_syntax_tree_json, x) == "ImportFrom"
    }
    direct_import_values = {
        AttributeExtractor.get_by_attribute(abstract_syntax_tree_json, x.replace("._type", ".names") + ".0.name")
        for x in import_type_attributes
        if AttributeExtractor.get_by_attribute(abstract_syntax_tree_json, x) == "Import"
    }
    all_names_to_import = import_from_values | direct_import_values
    return [x for x in all_names_to_import if x.startswith("odw")]


def generate_dag(module_map: Dict[str, str], root: str = None):
    """
    Generate a direct acyclic graph of the python files

    :param Dict[str, Any] module_map: Map of the form <moduel.name: python module content>
    :param str root: Drop all files not associated to the root. Default includes all files
    """
    dag = {module: extract_imports_from_file(module_content) for module, module_content in module_map.items()}
    if root:
        root_as_module = root.replace(".py", "").replace(os.sep, ".")
        relations_to_explore = list(dag.get(root_as_module, None))
        if not relations_to_explore:
            raise ValueError(f"No modules could be found for '{root}'")
        seen_dependencies = {root_as_module}
        while relations_to_explore:
            next_relation = relations_to_explore.pop()
            seen_dependencies.add(next_relation)
            new_relation_candidates = [x for x in dag[next_relation] if x not in seen_dependencies]
            relations_to_explore += new_relation_candidates
        dag = {k: v for k, v in dag.items() if k in seen_dependencies}
    return dag


def topological_sort(dag: dict[str, list[str]]) -> List[str]:
    return list(TopologicalSorter(dag).static_order())


def remove_odw_imports(module_content: str):
    cleaned_module_content = re.sub(r"(from\sodw.[A-Za-z._]*\simport\s[A-Za-z0-9.-]*)", r"#COMMENTOUT \1", module_content)
    cleaned_module_content = re.sub(r"(import\sodw[A-Za-z0-9._]*?)", r"#COMMENTOUT \1", cleaned_module_content)
    return cleaned_module_content


def construct_combined_notebook(root: str = None):
    module_content = get_all_odw_package_content()
    dag = generate_dag(module_content, root)
    module_imports_sorted = topological_sort(dag)
    combined_content = ""
    for module in module_imports_sorted:
        combined_content += f"### Module {module}\n\n{module_content[module]}\n\n"
    combined_content = combined_content.strip()
    cleaned_content = remove_odw_imports(combined_content)
    with open("odw/flatten_package_into_notebook__output.py", "w") as f:
        f.write(cleaned_content)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-r", "--root", help="The root file. Optional", default=None)
    args = parser.parse_args()
    root = args.root
    construct_combined_notebook(root)
