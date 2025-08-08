from mock import Mock
import sys
import types


"""
This overrides the import of notebookutils, which is essential for testing locally

Usage:
- Import `notebookutils` from this module BEFORE the real import. Use odw/test/unit_test/util/test_logging_util.py as an example
"""
# This overrides the import of notebookutils, which is essential for testing locally
module_name = "notebookutils"
notebookutils = types.ModuleType(module_name)
sys.modules[module_name] = notebookutils
notebookutils.mssparkutils = types.ModuleType(module_name+".mssparkutils")
notebookutils.mssparkutils.runtime = Mock(name=module_name+".runtime")
