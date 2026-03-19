"""Tests for XCom-related models"""

from unittest.mock import MagicMock

import pytest

from airflow_api_client.models import (
    DAG,
    DAGRun,
    ResourceSpec,
    TaskInstance,
    XComEntry,
    XComEntryList,
)

# pylint: disable=protected-access,duplicate-code

# pylint: disable=protected-access,duplicate-code


def test_xcom_spec_from_string_full(mock_app):
    """Test parsing full XCom spec with all components"""
    spec = ResourceSpec.from_string(mock_app, "my_dag/run_123/my_task/my_key")

    assert spec.dag.id == "my_dag"
    assert spec.dag_run.id == "run_123"
    assert spec.task_instance.id == "my_task"
    assert spec.key == "my_key"


def test_xcom_spec_from_string_no_key(mock_app):
    """Test parsing XCom spec without key"""
    spec = ResourceSpec.from_string(mock_app, "my_dag/run_123/my_task")

    assert spec.dag.id == "my_dag"
    assert spec.dag_run.id == "run_123"
    assert spec.task_instance.id == "my_task"
    assert spec.key is None


def test_xcom_spec_from_string_no_task(mock_app):
    """Test parsing XCom spec without task"""
    spec = ResourceSpec.from_string(mock_app, "my_dag/run_123")

    assert spec.dag.id == "my_dag"
    assert spec.dag_run.id == "run_123"
    assert spec.task_instance is None
    assert spec.key is None


def test_xcom_spec_from_string_invalid(mock_app):
    """Test that invalid spec raises ValueError"""
    with pytest.raises(ValueError, match="Invalid specification format"):
        ResourceSpec.from_string(mock_app, "only_dag_id")


def test_xcom_spec_repr(mock_app):
    """Test XComSpec string representation"""
    spec = ResourceSpec.from_string(mock_app, "my_dag/run_123/my_task/my_key")
    repr_str = repr(spec)

    assert "my_dag" in repr_str
    assert "run_123" in repr_str
    assert "my_key" in repr_str


def test_xcom_entry_initialization(mock_app):
    """Test XComEntry initialization"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    entry = XComEntry(mock_app, dag, dag_run, task, "my_key")

    assert entry.app == mock_app
    assert entry.dag == dag
    assert entry.dag_run == dag_run
    assert entry.task_instance == task
    assert entry.key == "my_key"
    assert entry._xcom_data is None


def test_xcom_entry_lazy_load(mock_app, mock_xcom_entry):
    """Test that XCom data is lazily loaded"""
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key")

    # Not fetched yet
    mock_app.client._xCom_api.get_xcom_entry.assert_not_called()

    # Access xcom_data property
    data = entry.xcom_data

    # Now fetched
    mock_app.client._xCom_api.get_xcom_entry.assert_called_once_with(
        dag_id="test_dag", task_id="test_task", dag_run_id="run_123", xcom_key="my_key"
    )
    assert data == mock_xcom_entry


def test_xcom_entry_getattr_delegation(mock_app, mock_xcom_entry):
    """Test that attribute access is delegated to xcom_data"""
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key")

    # Access delegated attributes
    assert entry.value == mock_xcom_entry.value
    assert entry.timestamp == mock_xcom_entry.timestamp


def test_xcom_entry_from_key(mock_app, mock_xcom_entry):
    """Test creating XComEntry from key"""
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    entry = XComEntry.from_key(mock_app, dag, task, dag_run, "my_key")

    assert entry.key == "my_key"
    assert entry._xcom_data == mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.assert_called_once()


def test_xcom_entry_list_initialization(mock_app):
    """Test XComEntryList initialization"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    xcom_list = XComEntryList(mock_app, dag, dag_run)

    assert xcom_list.app == mock_app
    assert xcom_list.dag == dag
    assert xcom_list.dag_run == dag_run
    assert xcom_list.xcoms == []


def test_xcom_entry_list_add(mock_app, mock_xcom_entry):
    """Test adding entries to XComEntryList"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key", xcom_data=mock_xcom_entry)

    xcom_list.add(entry)

    assert len(xcom_list.xcoms) == 1
    assert xcom_list.xcoms[0] == entry


def test_xcom_entry_list_iteration(mock_app, mock_xcom_entry):
    """Test iterating over XComEntryList"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry1 = XComEntry(mock_app, dag, dag_run, task, "key1", xcom_data=mock_xcom_entry)
    entry2 = XComEntry(mock_app, dag, dag_run, task, "key2", xcom_data=mock_xcom_entry)

    xcom_list.add(entry1)
    xcom_list.add(entry2)

    entries = list(xcom_list)
    assert len(entries) == 2
    assert entries[0] == entry1
    assert entries[1] == entry2


def test_xcom_entry_list_to_dict(mock_app, mock_xcom_entry):
    """Test converting XComEntryList to dictionary"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key", xcom_data=mock_xcom_entry)
    xcom_list.add(entry)

    result = xcom_list.to_dict()

    assert result["dag_id"] == "test_dag"
    assert result["dag_run_id"] == "run_123"
    assert "tasks" in result
    assert "test_task" in result["tasks"]
    assert "my_key" in result["tasks"]["test_task"]


def test_xcom_entry_list_output_json(mock_app, mock_xcom_entry, monkeypatch):
    """Test XComEntryList output in JSON format"""

    mock_output = MagicMock()
    monkeypatch.setattr("airflow_api_client.models.output", mock_output)

    mock_app.args.output_format = "json"
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key", xcom_data=mock_xcom_entry)
    xcom_list.add(entry)

    xcom_list.output()

    # Should call output with dict
    mock_output.assert_called_once()
    call_args = mock_output.call_args[0]
    assert isinstance(call_args[0], dict)
    assert call_args[0]["dag_id"] == "test_dag"


def test_xcom_entry_list_output_yaml(mock_app, mock_xcom_entry, monkeypatch):
    """Test XComEntryList output in YAML format"""

    mock_output = MagicMock()
    monkeypatch.setattr("airflow_api_client.models.output", mock_output)

    mock_app.args.output_format = "yaml"
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key", xcom_data=mock_xcom_entry)
    xcom_list.add(entry)

    xcom_list.output()

    # Should call output with dict
    mock_output.assert_called_once()
    call_args = mock_output.call_args[0]
    assert isinstance(call_args[0], dict)


def test_xcom_entry_list_output_human(mock_app, mock_xcom_entry, monkeypatch):
    """Test XComEntryList output in human-readable format"""

    mock_output = MagicMock()
    monkeypatch.setattr("airflow_api_client.models.output", mock_output)

    mock_app.args.output_format = "human"
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry = XComEntry(mock_app, dag, dag_run, task, "my_key", xcom_data=mock_xcom_entry)
    xcom_list.add(entry)

    xcom_list.output()

    # Should call output with list and headers
    mock_output.assert_called_once()
    call_args = mock_output.call_args
    assert isinstance(call_args[0][0], list)
    assert "headers" in call_args[1]


def test_xcom_entry_list_empty(mock_app):
    """Test empty XComEntryList"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    xcom_list = XComEntryList(mock_app, dag, dag_run)

    assert len(xcom_list.xcoms) == 0
    assert not list(xcom_list)
    assert xcom_list.to_dict() == {"dag_id": "test_dag", "dag_run_id": "run_123", "tasks": {}}


def test_xcom_entry_list_multiple_tasks(mock_app, mock_xcom_entry):
    """Test XComEntryList with entries from different tasks"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task1 = TaskInstance(mock_app, dag, dag_run, "task1")
    task2 = TaskInstance(mock_app, dag, dag_run, "task2")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry1 = XComEntry(mock_app, dag, dag_run, task1, "key1", xcom_data=mock_xcom_entry)
    entry2 = XComEntry(mock_app, dag, dag_run, task2, "key2", xcom_data=mock_xcom_entry)

    xcom_list.add(entry1)
    xcom_list.add(entry2)

    result = xcom_list.to_dict()
    assert len(result["tasks"]) == 2
    assert "task1" in result["tasks"]
    assert "task2" in result["tasks"]


def test_xcom_entry_list_multiple_keys_same_task(mock_app, mock_xcom_entry):
    """Test XComEntryList with multiple keys from same task"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    task = TaskInstance(mock_app, dag, dag_run, "test_task")

    xcom_list = XComEntryList(mock_app, dag, dag_run)
    entry1 = XComEntry(mock_app, dag, dag_run, task, "key1", xcom_data=mock_xcom_entry)
    entry2 = XComEntry(mock_app, dag, dag_run, task, "key2", xcom_data=mock_xcom_entry)

    xcom_list.add(entry1)
    xcom_list.add(entry2)

    result = xcom_list.to_dict()
    assert len(result["tasks"]) == 1
    assert len(result["tasks"]["test_task"]) == 2
    assert "key1" in result["tasks"]["test_task"]
    assert "key2" in result["tasks"]["test_task"]
