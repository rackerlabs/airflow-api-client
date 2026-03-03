"""Tests for TaskInstance model"""

from unittest.mock import MagicMock

from airflow_api_client.models import DAG, DAGRun, TaskInstance, XComEntryList

# pylint: disable=protected-access,duplicate-code


def test_task_instance_initialization(mock_app):
    """Test TaskInstance initialization"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    assert ti.app == mock_app
    assert ti.dag == dag
    assert ti.dag_run == dag_run
    assert ti.id == "test_task"
    assert ti._task_instance is None


def test_task_instance_lazy_load(mock_app, mock_task_instance):
    """Test that task instance details are lazily loaded"""
    mock_app.client._task_instance_api.get_task_instance.return_value = mock_task_instance

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    # Not fetched yet
    mock_app.client._task_instance_api.get_task_instance.assert_not_called()

    # Access task_instance property
    details = ti.task_instance

    # Now fetched
    mock_app.client._task_instance_api.get_task_instance.assert_called_once_with(
        dag_id="test_dag", dag_run_id="run_123", task_id="test_task"
    )
    assert details == mock_task_instance


def test_task_instance_cached(mock_app, mock_task_instance):
    """Test that task instance details are cached"""
    mock_app.client._task_instance_api.get_task_instance.return_value = mock_task_instance

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    # Access twice
    _ = ti.task_instance
    _ = ti.task_instance

    # Should only fetch once
    mock_app.client._task_instance_api.get_task_instance.assert_called_once()


def test_task_instance_getattr_delegation(mock_app, mock_task_instance):
    """Test that attribute access is delegated to task instance details"""
    mock_app.client._task_instance_api.get_task_instance.return_value = mock_task_instance

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    # Access delegated attributes
    assert ti.state == mock_task_instance.state
    assert ti.task_display_name == mock_task_instance.task_display_name


def test_task_instance_get_xcom_entries_with_key(mock_app, mock_xcom_entry):
    """Test getting XCom entries with specific key"""
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    result = ti.get_xcom_entries(key="my_key")

    assert isinstance(result, XComEntryList)
    assert len(result.xcoms) == 1
    mock_app.client._xCom_api.get_xcom_entry.assert_called_once_with(
        dag_id="test_dag", task_id="test_task", dag_run_id="run_123", xcom_key="my_key"
    )


def test_task_instance_get_xcom_entries_all(mock_app, mock_xcom_entry):
    """Test getting all XCom entries for a task"""
    # Mock the collection response
    mock_collection = MagicMock()
    mock_entry = MagicMock()
    mock_entry.key = "key1"
    mock_collection.xcom_entries = [mock_entry]
    mock_app.client._xCom_api.get_xcom_entries.return_value = mock_collection

    # Mock the individual entry fetch
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    result = ti.get_xcom_entries()

    assert isinstance(result, XComEntryList)
    # Should fetch collection first
    mock_app.client._xCom_api.get_xcom_entries.assert_called_once_with(
        dag_id="test_dag", task_id="test_task", dag_run_id="run_123"
    )
    # Then fetch individual entry
    mock_app.client._xCom_api.get_xcom_entry.assert_called_once()


def test_task_instance_repr(mock_app):
    """Test TaskInstance string representation"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")
    ti = TaskInstance(mock_app, dag, dag_run, "test_task")

    repr_str = repr(ti)
    assert "test_task" in repr_str
    assert "test_dag" in repr_str
    assert "run_123" in repr_str
