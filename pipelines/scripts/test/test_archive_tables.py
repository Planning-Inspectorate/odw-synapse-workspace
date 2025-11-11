from pipelines.scripts.archive_tables import TableArchiveUtil, SparkSchemaCallAnalyserFactory
from typing import Callable, Dict, Any
import mock
import pytest
import os



class MockSparkSchemaCallAnalyser():
    def get_notebook(self):
        pass

    def get_inputs_to_py_create_spark_schema(self, activity_run: Dict[str, Any]) -> Dict[str, str]:
        pass


@pytest.mark.parametrize(
    "artifact_function",
    [
        TableArchiveUtil.get_all_unarchived_notebooks,
        TableArchiveUtil.get_all_unarchived_pipelines
    ]
)
def test__table_archiver__get_all_unarchived_artifacts(artifact_function: Callable):
    """
    - Given there are 4 artifacts in the target folder, and that two of these files have the word `archive` in their folder.name property
    - When I call `get_all_unarchived_notebooks` or `get_all_unarchived_pipelines`
    - Then only two of the artifact names are returned (the two without the word `archive` in their folder.name properties)
    """
    mock_listdir = [
        "file_a.json",
        "file_b.json",
        "file_c.json",
        "file_d.json"
    ]
    file_open_side_effects = [
        mock.mock_open(read_data='{"name": "file_a", "properties": {"folder": {"name": ""}}}').return_value,
        mock.mock_open(read_data='{"name": "file_b", "properties": {"folder": {"name": "archive"}}}').return_value,
        mock.mock_open(read_data='{"name": "file_c", "properties": {"folder": {"name": "archive/some_folder"}}}').return_value,
        mock.mock_open(read_data='{"name": "file_d", "properties": {"folder": {"name": "some_folder"}}}').return_value,
    ]
    expected_output = {
        "file_a",
        "file_d"
    }
    with mock.patch.object(os, "listdir", return_value=mock_listdir):
        with mock.patch('builtins.open', side_effect=file_open_side_effects):
            with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                inst = TableArchiveUtil()
                actual_output = artifact_function(inst)
                assert expected_output == actual_output


def test__table_archiver__get_all_unarchived_files_that_use_py_create_spark_schema():
    """
    - Given I have 5 notebooks (A, B, C, D, E), with references in the form `py_create_spark_schema` <- B <- C <- D <- E
    - When I call `get_all_unarchived_files_that_use_py_create_spark_schema`
    - Then I should receive `{"py_create_spark_schema", "B", "C", "D", "E"}
    """
    mock_unarchived_notebooks = [
        "notebook_a",  # No ref
        "notebook_b",  # Direct ref
        "notebook_c",  # Indirect ref 1
        "notebook_d",  # Indirect ref 2
        "notebook_e"  # Indirect ref 3
    ]
    file_open_side_effects = [
        mock.mock_open(read_data='{"name": "notebook_a", "properties": ""}').return_value,  # No ref
        mock.mock_open(read_data='{"name": "notebook_b", "properties": "py_create_spark_schema"}').return_value,  # Direct ref
        mock.mock_open(read_data='{"name": "notebook_c", "properties": "notebook_b"}').return_value,  # Indirect ref 1
        mock.mock_open(read_data='{"name": "notebook_d", "properties": "notebook_c"}').return_value,  # Indirect ref 2
        mock.mock_open(read_data='{"name": "notebook_e", "properties": "notebook_d"}').return_value  # Indirect ref 2
    ]
    expected_output = {
        "py_create_spark_schema",  # This notebook should always be included in the result
        "notebook_b",
        "notebook_c",
        "notebook_d",
        "notebook_e"
    }
    with mock.patch.object(TableArchiveUtil, "get_all_unarchived_notebooks", return_value=mock_unarchived_notebooks):
        with mock.patch('builtins.open', side_effect=file_open_side_effects):
            with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                actual_output = TableArchiveUtil().get_all_unarchived_files_that_use_py_create_spark_schema()
                assert expected_output == actual_output


def test__table_archiver__get_all_notebook_calls_for_pipeline():
    """
    - Given we have a `main_pipeline` that calls `child_pipeline_a` twice, and `child_pipeline_a` calls `child_pipeline_b`,
    and `main_pipeline` calls `notebook_a` twice, and `child_pipeline_a` calls `notebook_call_b` once
    - When I call `get_all_notebook_calls_for_pipeline`
    - Then a dictionary which maps each pipeline to their called notebooks should be returned
    """
    get_child_pipeline_runs_side_effects = [
        # Called directly by the master pipeline
        [
            {
                "output": {
                    "pipelineName": "child_pipeline_a",
                    "pipelineRunId": "child_pipeline_a_run_id_a"
                }
            },
            {
                "output": {
                    "pipelineName": "child_pipeline_a",
                    "pipelineRunId": "child_pipeline_a_run_id_b"
                }
            }
        ],
        # Called by one of the first-order children of the master pipeline
        [
            {
                "output": {
                    "pipelineName": "child_pipeline_b",
                    "pipelineRunId": "child_pipeline_b_run_id_a"
                }
            }
        ],
        [],
        []
    ]
    get_notebook_calls_side_effects = [
        [
            "notebook_a_call_a",
            "notebook_a_call_b"
        ],
        [
            "notebook_call_b_call_a"
        ],
        [],
        []
    ]
    expected_output = {
        "main_pipeline": [
            "notebook_a_call_a",
            "notebook_a_call_b"
        ],
        "child_pipeline_a": [
            "notebook_call_b_call_a"
        ]
    }
    with mock.patch.object(TableArchiveUtil, "get_child_pipeline_runs", side_effect=get_child_pipeline_runs_side_effects):
        with mock.patch.object(TableArchiveUtil, "get_notebook_calls", side_effect=get_notebook_calls_side_effects):
            with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                actual_output = TableArchiveUtil().get_all_notebook_calls_for_pipeline("main_pipeline", "")
                assert expected_output == actual_output


def test__table_archiver__get_relevant_notebook_calls_for_pipeline():
    """
    - Given I have `3` notebooks called by pipelines (in dict format), and that two of these notebooks are considered "relevant"
    - When I call `get_relevant_notebook_calls_for_pipeline`
    - Then I should receive a copy of my original dictionary with only the relevant notebooks
    """
    mock_relevant_files = {
        "notebook_a_call_b",
        "notebook_call_b_call_a"
    }
    mock_notebook_calls_for_pipeline = {
        "main_pipeline": [
            # The first one should be dropped, because it is not included in mock_relevant_files
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_a_call_a"
                    }
                }
            },
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_a_call_b"
                    }
                }
            }
        ],
        "child_pipeline_a": [
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_call_b_call_a"
                    }
                }
            }
        ]
    }
    expected_output = {
        "main_pipeline": [
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_a_call_b"
                    }
                }
            }
        ],
        "child_pipeline_a": [
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_call_b_call_a"
                    }
                }
            }
        ]
    }
    with mock.patch.object(TableArchiveUtil, "get_all_unarchived_files_that_use_py_create_spark_schema", return_value=mock_relevant_files):
        with mock.patch.object(TableArchiveUtil, "get_all_notebook_calls_for_pipeline", return_value=mock_notebook_calls_for_pipeline):
            with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                actual_output = TableArchiveUtil().get_relevant_notebook_calls_for_pipeline("", "")
                assert expected_output == actual_output


def test__table_archiver__get_tables_referenced_by_notebooks():
    """
    - Given I have a pipeline called `pipeine_that_calls_notebooks` that calls 2 notebooks, each that interact with a separate table
    - When I call `get_tables_referenced_by_notebooks`
    - Then both of the interacted tables should be returned, in the format `db_name.table_name`
    """
    mock_notebook_calls = {
        "pipeine_that_calls_notebooks": [
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_a"
                    }
                }
            },
            {
                "input": {
                    "notebook": {
                        "referenceName": "notebook_b"
                    }
                }
            }
        ]
    }
    analyser_side_effects = [
        MockSparkSchemaCallAnalyser,
        MockSparkSchemaCallAnalyser
    ]
    inputs_side_effects = [
        {"db_name": "db_a", "entity_name": "table_a"},
        {"db_name": "db_b", "entity_name": "table-b"}
    ]
    expected_output = {
        "db_a.table_a",
        "db_b.table_b"
    }
    with mock.patch.object(SparkSchemaCallAnalyserFactory, "get", side_effect=analyser_side_effects):
        with mock.patch.object(MockSparkSchemaCallAnalyser, "get_inputs_to_py_create_spark_schema", side_effect=inputs_side_effects):
            with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                actual_output = TableArchiveUtil().get_tables_referenced_by_notebooks(mock_notebook_calls)
                assert expected_output == actual_output


def test__table_archiver__get_tables_referenced_by_code():
    """
    - Given I have 6 tables to archive, and that across 3 notebooks and 1 pipeline, 3 of the tables are references
    - When I call `get_tables_referenced_by_code`
    - Then I should receive a set with only these three tables
    """
    mock_tables_to_archive = [
        "db_a.table_a",
        "db_a.table_b",
        "db_b.table_a",
        "db_b.table_b",
        "db_c.table_a",
        "db_c.table_b"
    ]
    mock_unarchived_notebooks = {
        "notebook_a",
        "notebook_b",
        "notebook_c"
    }
    mock_unarchived_pipelines = {
        "pipeline_a"
    }
    file_open_side_effects = [
        mock.mock_open(read_data="Some notebook that doesnt call any important tables").return_value,
        mock.mock_open(read_data="Some notebook that calls db_a.table_a").return_value,
        mock.mock_open(read_data="Some notebook that calls table_a that previously references db_b").return_value,
        mock.mock_open(read_data="Some pipeline that calls db_c.table_a").return_value,
    ]
    expected_output = {
        "db_a.table_a",
        "db_b.table_a",
        "db_c.table_a"
    }
    with mock.patch.object(TableArchiveUtil, "get_all_unarchived_notebooks", return_value=mock_unarchived_notebooks):
        with mock.patch.object(TableArchiveUtil, "get_all_unarchived_pipelines", return_value=mock_unarchived_pipelines):
            with mock.patch('builtins.open', side_effect=file_open_side_effects):
                with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                    actual_output = TableArchiveUtil().get_tables_referenced_by_code(mock_tables_to_archive)
                    assert expected_output == actual_output


def test__table_archiver__get_table_analysis_result():
    """
    - Given we have `9` tables total across all databases, `3` are referenced by notebook calls to
    `py_create_spark_schema` and `1` is referenced directly in the code
    - When we call get_table_analysis_result
    - Then 4 of these tables should be identified to be kept, and the remaining 5 should be marked for archival
    """
    mock_get_relevant_notebook_calls_for_pipeline = [
        {
            "pipeline_a": [
                {
                    "name": "notebook_a"
                },
                {
                    "name": "notebook_b"
                }
            ],
            "pipeline_b": [
                {
                    "name": "notebook_c"
                },
                {
                    "name": "notebook_d"
                }
            ]
        },
        {
            "pipeline_c": [
                {
                    "name": "notebook_e"
                }
            ]
        }
    ]
    mock_get_all_tables = {
        "database_a.table_a",
        "database_a.table_b",
        "database_a.table_c",
        "database_b.table_a",
        "database_b.table_b",
        "database_b.table_c",
        "database_c.table_a",
        "database_c.table_b",
        "database_c.table_c"
    }
    mock_get_tables_referenced_by_notebooks = {
        "database_a.table_a",
        "database_b.table_b",
        "database_c.table_c"
    }
    mock_get_tables_referenced_by_code = {
        "database_a.table_c"
    }
    expected_output = {
        "tables_to_keep": [
            "database_a.table_a",
            "database_a.table_c",
            "database_b.table_b",
            "database_c.table_c"
        ],
        "tables_to_archive": [
            "database_a.table_b",
            "database_b.table_a",
            "database_b.table_c",
            "database_c.table_a",
            "database_c.table_b"
        ]
    }
    with mock.patch.object(TableArchiveUtil, "get_relevant_notebook_calls_for_pipeline", side_effect=mock_get_relevant_notebook_calls_for_pipeline):
        with mock.patch.object(TableArchiveUtil, "get_tables_referenced_by_notebooks", return_value=mock_get_tables_referenced_by_notebooks):
            with mock.patch.object(TableArchiveUtil, "get_all_tables", return_value=mock_get_all_tables):
                with mock.patch.object(TableArchiveUtil, "get_tables_referenced_by_code", return_value=mock_get_tables_referenced_by_code):
                    with mock.patch.object(TableArchiveUtil, "__init__", return_value=None):
                        actual_output = TableArchiveUtil().get_table_analysis_result("a", "b")
                        assert expected_output == actual_output
