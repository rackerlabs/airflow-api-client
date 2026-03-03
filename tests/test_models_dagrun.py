"""Tests for DAGRun model"""

from unittest.mock import MagicMock

from airflow_api_client.models import DAG, DAGRun, TaskInstance, XComEntryList

# pylint: disable=protected-access,import-outside-toplevel


# pylint: disable=protected-access,duplicate-code


def test_dagrun_initialization(mock_app):
    """Test DAGRun initialization"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    assert dag_run.app == mock_app
    assert dag_run.dag == dag
    assert dag_run.id == "run_123"
    assert dag_run._dag_run is None
    assert dag_run._has_finished is False


def test_dagrun_lazy_load(mock_app):
    """Test that DAG run details are lazily loaded"""
    mock_dag_run = MagicMock()
    mock_dag_run.state = "success"
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Not fetched yet
    mock_app.client._DAG_run_api.get_dag_run.assert_not_called()

    # Access dag_run property
    details = dag_run.dag_run

    # Now fetched
    mock_app.client._DAG_run_api.get_dag_run.assert_called_once_with(
        dag_id="test_dag", dag_run_id="run_123"
    )
    assert details == mock_dag_run


def test_dagrun_cached(mock_app):
    """Test that DAG run details are cached when finished"""
    mock_dag_run = MagicMock()
    mock_dag_run.state = "success"
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access state to mark as finished
    _ = dag_run.state

    # Access dag_run again
    _ = dag_run.dag_run

    # Should only fetch once since it's finished
    mock_app.client._DAG_run_api.get_dag_run.assert_called_once()


def test_dagrun_getattr_delegation(mock_app):
    """Test that attribute access is delegated to DAG run details"""
    mock_dag_run = MagicMock()
    mock_dag_run.state = "success"
    mock_dag_run.start_date = "2026-03-03T10:00:00Z"
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access delegated attributes
    assert dag_run.state == "success"
    assert dag_run.start_date == "2026-03-03T10:00:00Z"


def test_dagrun_task_instances(mock_app):
    """Test getting task instances for a DAG run"""
    mock_collection = MagicMock()
    mock_ti = MagicMock()
    mock_ti.task_id = "task1"
    mock_collection.task_instances = [mock_ti]
    mock_app.client._task_instance_api.get_task_instances.return_value = mock_collection

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    task_instances = dag_run.task_instances

    assert len(task_instances) == 1
    assert isinstance(task_instances[0], TaskInstance)
    assert task_instances[0].id == "task1"
    mock_app.client._task_instance_api.get_task_instances.assert_called_once_with(
        dag_id="test_dag", dag_run_id="run_123"
    )


def test_dagrun_task_instances_not_cached(mock_app):
    """Test that task instances are NOT cached (fetched each time)"""
    mock_collection = MagicMock()
    mock_collection.task_instances = []
    mock_app.client._task_instance_api.get_task_instances.return_value = mock_collection

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access twice
    _ = dag_run.task_instances
    _ = dag_run.task_instances

    # Should fetch twice (not cached)
    assert mock_app.client._task_instance_api.get_task_instances.call_count == 2


def test_dagrun_xcoms(mock_app, mock_xcom_entry):
    """Test getting all XCom entries for a DAG run"""
    # Mock task instances
    mock_ti_collection = MagicMock()
    mock_ti = MagicMock()
    mock_ti.task_id = "task1"
    mock_ti_collection.task_instances = [mock_ti]
    mock_app.client._task_instance_api.get_task_instances.return_value = mock_ti_collection

    # Mock XCom entries
    mock_xcom_collection = MagicMock()
    mock_xcom = MagicMock()
    mock_xcom.key = "key1"
    mock_xcom_collection.xcom_entries = [mock_xcom]
    mock_app.client._xCom_api.get_xcom_entries.return_value = mock_xcom_collection

    # Mock individual XCom fetch
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    xcoms = dag_run.xcoms

    assert isinstance(xcoms, XComEntryList)
    # Should fetch task instances first
    mock_app.client._task_instance_api.get_task_instances.assert_called_once()


def test_dagrun_repr(mock_app):
    """Test DAGRun string representation"""
    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    repr_str = repr(dag_run)
    assert "run_123" in repr_str
    assert "test_dag" in repr_str


def test_dagrun_state_running(mock_app):
    """Test DAGRun state property when DAG is still running"""
    from airflow_client.client.models.dag_run_state import DagRunState

    mock_dag_run = MagicMock()
    mock_dag_run.state = DagRunState.RUNNING
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access state twice
    state1 = dag_run.state
    state2 = dag_run.state

    assert state1 == DagRunState.RUNNING
    assert state2 == DagRunState.RUNNING

    # Should fetch twice since it's not finished
    assert mock_app.client._DAG_run_api.get_dag_run.call_count == 2


def test_dagrun_state_success_cached(mock_app):
    """Test DAGRun state property caches when in terminal state (success)"""
    from airflow_client.client.models.dag_run_state import DagRunState

    mock_dag_run = MagicMock()
    mock_dag_run.state = DagRunState.SUCCESS
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access state twice
    state1 = dag_run.state
    state2 = dag_run.state

    assert state1 == DagRunState.SUCCESS
    assert state2 == DagRunState.SUCCESS

    # Should only fetch once since it's finished
    mock_app.client._DAG_run_api.get_dag_run.assert_called_once()
    assert dag_run._has_finished is True


def test_dagrun_state_failed_cached(mock_app):
    """Test DAGRun state property caches when in terminal state (failed)"""
    from airflow_client.client.models.dag_run_state import DagRunState

    mock_dag_run = MagicMock()
    mock_dag_run.state = DagRunState.FAILED
    mock_app.client._DAG_run_api.get_dag_run.return_value = mock_dag_run

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    # Access state twice
    state1 = dag_run.state
    state2 = dag_run.state

    assert state1 == DagRunState.FAILED
    assert state2 == DagRunState.FAILED

    # Should only fetch once since it's finished
    mock_app.client._DAG_run_api.get_dag_run.assert_called_once()
    assert dag_run._has_finished is True


def test_dagrun_xcoms_integration(mock_app, mock_xcom_entry):
    """Test DAGRun.xcoms property fetches all XComs for the run"""
    # Mock task instances
    mock_ti_collection = MagicMock()
    mock_ti1 = MagicMock()
    mock_ti1.task_id = "task1"
    mock_ti2 = MagicMock()
    mock_ti2.task_id = "task2"
    mock_ti_collection.task_instances = [mock_ti1, mock_ti2]
    mock_app.client._task_instance_api.get_task_instances.return_value = mock_ti_collection

    # Mock XCom entries for each task
    mock_xcom_collection = MagicMock()
    mock_xcom = MagicMock()
    mock_xcom.key = "return_value"
    mock_xcom_collection.xcom_entries = [mock_xcom]
    mock_app.client._xCom_api.get_xcom_entries.return_value = mock_xcom_collection

    # Mock individual XCom fetch
    mock_response = MagicMock()
    mock_response.actual_instance = mock_xcom_entry
    mock_app.client._xCom_api.get_xcom_entry.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    xcoms = dag_run.xcoms

    # Should return XComEntryList
    assert isinstance(xcoms, XComEntryList)

    # Should fetch task instances
    mock_app.client._task_instance_api.get_task_instances.assert_called_once_with(
        dag_id="test_dag", dag_run_id="run_123"
    )

    # Should fetch XCom entries for each task
    assert mock_app.client._xCom_api.get_xcom_entries.call_count == 2


def test_dagrun_xcoms_empty(mock_app):
    """Test DAGRun.xcoms property with no task instances"""
    # Mock empty task instances
    mock_ti_collection = MagicMock()
    mock_ti_collection.task_instances = []
    mock_app.client._task_instance_api.get_task_instances.return_value = mock_ti_collection

    dag = DAG(mock_app, "test_dag")
    dag_run = DAGRun(mock_app, dag, "run_123")

    xcoms = dag_run.xcoms

    # Should return empty XComEntryList
    assert isinstance(xcoms, XComEntryList)
    assert len(xcoms.xcoms) == 0
