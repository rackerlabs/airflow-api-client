"""Tests for DAGBag model"""

from unittest.mock import MagicMock

from airflow_api_client.models import DAG, DAGBag

# pylint: disable=protected-access,duplicate-code


def test_dagbag_initialization(mock_app):
    """Test DAGBag initialization"""
    dagbag = DAGBag(mock_app)

    assert dagbag.app == mock_app


def test_dagbag_get_all_dags_without_details(mock_app):
    """Test getting all DAGs without details"""
    mock_collection = MagicMock()
    mock_dag1 = MagicMock()
    mock_dag1.dag_id = "dag1"
    mock_dag2 = MagicMock()
    mock_dag2.dag_id = "dag2"
    mock_collection.dags = [mock_dag1, mock_dag2]
    mock_app.client._DAG_api.get_dags.return_value = mock_collection

    dagbag = DAGBag(mock_app)
    dags = dagbag.get_all_dags(details=False)

    assert len(dags) == 2
    assert all(isinstance(dag, DAG) for dag in dags)
    assert dags[0].id == "dag1"
    assert dags[1].id == "dag2"

    # Should only call get_dags, not get_dag_details
    mock_app.client._DAG_api.get_dags.assert_called_once()
    mock_app.client._DAG_api.get_dag_details.assert_not_called()


def test_dagbag_get_all_dags_with_details(mock_app, mock_dag_details):
    """Test getting all DAGs with details"""
    mock_collection = MagicMock()
    mock_dag1 = MagicMock()
    mock_dag1.dag_id = "dag1"
    mock_dag2 = MagicMock()
    mock_dag2.dag_id = "dag2"
    mock_collection.dags = [mock_dag1, mock_dag2]
    mock_app.client._DAG_api.get_dags.return_value = mock_collection
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    dagbag = DAGBag(mock_app)
    dags = dagbag.get_all_dags(details=True)

    assert len(dags) == 2
    assert all(isinstance(dag, DAG) for dag in dags)

    # Should call get_dags once and get_dag_details twice
    mock_app.client._DAG_api.get_dags.assert_called_once()
    assert mock_app.client._DAG_api.get_dag_details.call_count == 2


def test_dagbag_get_all_dags_with_kwargs(mock_app):
    """Test getting all DAGs with additional query parameters"""
    mock_collection = MagicMock()
    mock_collection.dags = []
    mock_app.client._DAG_api.get_dags.return_value = mock_collection

    dagbag = DAGBag(mock_app)
    dags = dagbag.get_all_dags(details=False, limit=10, offset=5)

    # Should pass kwargs to get_dags
    mock_app.client._DAG_api.get_dags.assert_called_once_with(limit=10, offset=5)
    assert dags == []


def test_dagbag_get_all_dags_default_details(mock_app, mock_dag_details):
    """Test that details=True is the default"""
    mock_collection = MagicMock()
    mock_dag = MagicMock()
    mock_dag.dag_id = "dag1"
    mock_collection.dags = [mock_dag]
    mock_app.client._DAG_api.get_dags.return_value = mock_collection
    mock_app.client._DAG_api.get_dag_details.return_value = mock_dag_details

    dagbag = DAGBag(mock_app)
    dags = dagbag.get_all_dags()  # No details parameter

    assert dags[0].id == "dag1"

    # Should fetch details by default
    mock_app.client._DAG_api.get_dag_details.assert_called_once()
