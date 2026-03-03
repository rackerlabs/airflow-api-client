"""Shared pytest fixtures"""

from unittest.mock import MagicMock

import pytest

from airflow_api_client.typedefs import App

# pylint: disable=protected-access,duplicate-code


@pytest.fixture
def mock_app():
    """Create a mock App instance with mocked client"""
    app = MagicMock(spec=App)
    app.client = MagicMock()
    app.client._DAG_api = MagicMock()
    app.client._DAG_run_api = MagicMock()
    app.client._xCom_api = MagicMock()
    app.client._task_instance_api = MagicMock()
    app.args = MagicMock()
    app.args.output_format = "human"
    app.args.verbose = 0
    return app


@pytest.fixture
def mock_dag_details():
    """Create mock DAG details response"""
    details = MagicMock()
    details.dag_id = "test_dag"
    details.dag_display_name = "Test DAG"
    details.is_paused = False
    details.params = {
        "param1": {
            "description": "Test parameter",
            "schema": {"type": ["string"]},
        }
    }
    return details


@pytest.fixture
def mock_task_instance():
    """Create mock task instance response"""
    ti = MagicMock()
    ti.task_id = "test_task"
    ti.task_display_name = "Test Task"
    ti.id = "019cafcc-8a66-7ce9-8fd5-e4b79d315f6c"
    ti.state = "success"
    return ti


@pytest.fixture
def mock_xcom_entry():
    """Create mock XCom entry response"""
    xcom = MagicMock()
    xcom.key = "return_value"
    xcom.value = {"result": "test"}
    xcom.task_id = "test_task"
    xcom.task_display_name = "Test Task"
    xcom.timestamp = "2026-03-03T12:00:00Z"
    return xcom
