from mock import Mock
import sys
import types


"""
This overrides the import of notebookutils, which is essential for testing locally.

This is based on functionality defined at https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities

Usage:
- Import `notebookutils` from this module BEFORE the real import. Use odw/test/unit_test/util/test_logging_util.py as an example
"""


class NotebookExit(Exception):
    pass


# This overrides the import of notebookutils, which is essential for testing locally
module_name = "notebookutils"
# All submodules of notebookutils. Each leaf submodule is overwritten to be a mock object, which can then be used in tests
submodules = [
    "mssparkutils.credentials",
    "mssparkutils.fs",
    "mssparkutils.lakehouse",
    "mssparkutils.notebook",
    "mssparkutils.runtime",
    "mssparkutils.session",
    "mssparkutils.udf",
    "mssparkutils.variableLibrary",
    "mssparkutils.handlers",
    "credentials",
    "fs",
    "lakehouse",
    "notebook",
    "runtime",
    "session",
    "udf",
    "variableLibrary",
]

notebookutils = types.ModuleType(module_name)
sys.modules[module_name] = notebookutils

# Dynamically mock all of notebookutils
for submodule in submodules:
    submodule_split = submodule.split(".")
    leaf_module_name = submodule_split.pop(-1)
    current_module = notebookutils
    current_module_name = module_name
    for module in submodule_split:
        current_module_name = ".".join([current_module_name, module])
        if not getattr(current_module, module, False):
            setattr(notebookutils, module, types.ModuleType(current_module_name))
        current_module = getattr(current_module, module)
    if not getattr(current_module, leaf_module_name, False):
        setattr(current_module, leaf_module_name, Mock(name=".".join([current_module_name, leaf_module_name])))

notebookutils.mssparkutils.handlers.notebookHandler.NotebookExit = NotebookExit
