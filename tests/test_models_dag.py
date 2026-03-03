"""Tests for DAG model"""

from unittest.mock import MagicMock

from airflow_api_client.models import DAG, DAGRun

# pylint: disable=protected-access, unused-argument


# pylint: disable=protected-access,duplicate-code


def test_dag_initialization(mock_app):
    """Test DAG initialization"""
    dag = DAG(mock_app, "test_dag")
    assert dag.id == "test_dag"
    assert dag.app == mock_app
    assert dag._dag is None


def test_dag_initialization_with_details(mock_app, mock_dag_details):
    """Test DAG initialization with pre-loaded details"""
    dag = DAG(mock_app, "test_dag", dag=mock_dag_details)
    assert dag.id == "test_dag"
    assert dag._dag == mock_dag_details


def test_dag_lazy_load_details(mock_app, mock_dag_details):
    """Test that DAG details are lazily loaded"""
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    dag = DAG(mock_app, "test_dag")

    # Details not fetched yet
    mock_app.client._DAG_api.get_dag_details.assert_not_called()

    # Access details property
    details = dag.dag

    # Now details are fetched
    mock_app.client._DAG_api.get_dag_details.assert_called_once_with(dag_id="test_dag")
    assert details == mock_dag_details


def test_dag_details_cached(mock_app, mock_dag_details):
    """Test that DAG details are cached after first fetch"""
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    dag = DAG(mock_app, "test_dag")

    # Access details twice
    _ = dag.dag
    _ = dag.dag

    # Should only fetch once
    mock_app.client._DAG_api.get_dag_details.assert_called_once()


def test_dag_getattr_delegation(mock_app, mock_dag_details):
    """Test that attribute access is delegated to DAG details"""
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    dag = DAG(mock_app, "test_dag")

    # Access attribute that exists on details
    assert dag.dag_display_name == "Test DAG"
    assert dag.is_paused is False


def test_dag_run_trigger(mock_app):
    """Test triggering a DAG run"""
    mock_response = MagicMock()
    mock_response.dag_run_id = "manual__123456"
    mock_app.client._DAG_run_api.trigger_dag_run.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    conf = {"conf": {"key": "value"}}

    dag_run = dag.run(conf=conf)

    # Verify API was called
    mock_app.client._DAG_run_api.trigger_dag_run.assert_called_once()
    call_args = mock_app.client._DAG_run_api.trigger_dag_run.call_args
    assert call_args.kwargs["dag_id"] == "test_dag"

    # Verify DAGRun object returned
    assert isinstance(dag_run, DAGRun)
    assert dag_run.dag == dag


def test_dag_run_without_config(mock_app):
    """Test triggering a DAG run without configuration"""
    mock_response = MagicMock()
    mock_app.client._DAG_run_api.trigger_dag_run.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = dag.run()

    # Should return a DAGRun instance
    assert isinstance(dag_run, DAGRun)
    assert dag_run.dag == dag
    assert dag_run.id.startswith("manual__")

    # Should call trigger_dag_run with correct payload
    mock_app.client._DAG_run_api.trigger_dag_run.assert_called_once()
    call_args = mock_app.client._DAG_run_api.trigger_dag_run.call_args
    assert call_args[1]["dag_id"] == "test_dag"

    # Check payload structure
    payload = call_args[1]["trigger_dag_run_post_body"]
    assert hasattr(payload, "dag_run_id")
    assert hasattr(payload, "logical_date")


def test_dag_run_with_config(mock_app):
    """Test triggering a DAG run with configuration"""
    mock_response = MagicMock()
    mock_app.client._DAG_run_api.trigger_dag_run.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    config = {"conf": {"key": "value"}}
    dag_run = dag.run(conf=config)
    assert dag_run.dag == dag

    # Should call trigger_dag_run with config in payload
    call_args = mock_app.client._DAG_run_api.trigger_dag_run.call_args
    payload = call_args[1]["trigger_dag_run_post_body"]
    assert payload.conf == {"key": "value"}


def test_dag_run_with_custom_run_id(mock_app):
    """Test triggering a DAG run with custom run ID"""
    mock_response = MagicMock()
    mock_app.client._DAG_run_api.trigger_dag_run.return_value = mock_response

    dag = DAG(mock_app, "test_dag")
    dag_run = dag.run(run_id="custom_run_123")

    # Should use the provided run_id
    assert dag_run.id == "custom_run_123"

    # Verify payload has custom run_id
    call_args = mock_app.client._DAG_run_api.trigger_dag_run.call_args
    payload = call_args[1]["trigger_dag_run_post_body"]
    assert payload.dag_run_id == "custom_run_123"


def test_dag_create_payload(mock_app, mock_dag_details, monkeypatch):
    """Test creating a payload from DAG parameters"""
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    # Mock user input
    inputs = iter(["test_value"])
    monkeypatch.setattr("builtins.input", lambda: next(inputs))

    dag = DAG(mock_app, "test_dag")
    payload = dag.create_payload()

    assert "conf" in payload
    assert payload["conf"]["param1"] == "test_value"


def test_dag_repr(mock_app):
    """Test DAG string representation"""
    dag = DAG(mock_app, "test_dag")
    assert "test_dag" in repr(dag)
