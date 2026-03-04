"""Tests for list command"""

from unittest.mock import MagicMock, patch

from airflow_api_client.commands.list import ListDagsCommand
from airflow_api_client.models import DAG, DAGRun

# pylint: disable=protected-access,duplicate-code


def test_list_dags_command(mock_app):
    """Test list dags subcommand"""
    mock_app.args.list_resource = "dags"

    # Mock DAGBag
    mock_dag = MagicMock(spec=DAG)
    mock_dag.dag_id = "test_dag"
    mock_dag.dag_display_name = "Test DAG"
    mock_dag.is_paused = False
    mock_dag.owners = ["airflow"]
    mock_dag.tags = []
    mock_dag.next_dagrun_logical_date = None

    with patch("airflow_api_client.commands.list.DAGBag") as mock_dagbag:
        mock_dagbag.return_value.get_all_dags.return_value = [mock_dag]

        cmd = ListDagsCommand()
        cmd._list_dags(mock_app)

        mock_dagbag.assert_called_once_with(mock_app)
        # Default is details=True
        mock_dagbag.return_value.get_all_dags.assert_called_once_with(details=True)


def test_list_dags_with_filter(mock_app):
    """Test list dags with filter"""
    mock_app.args.list_resource = "dags"
    mock_app.args.filter = ["is_paused == true"]

    # Mock two DAGs, one paused, one not
    mock_dag1 = MagicMock()
    mock_dag1.json.return_value = '{"dag_id": "dag1", "is_paused": true}'
    mock_dag1.dag_id = "dag1"
    mock_dag1.dag_display_name = "DAG 1"
    mock_dag1.is_paused = True
    mock_dag1.owners = ["airflow"]
    mock_dag1.tags = []
    mock_dag1.next_dagrun_logical_date = None

    mock_dag2 = MagicMock()
    mock_dag2.json.return_value = '{"dag_id": "dag2", "is_paused": false}'
    mock_dag2.dag_id = "dag2"
    mock_dag2.dag_display_name = "DAG 2"
    mock_dag2.is_paused = False
    mock_dag2.owners = ["airflow"]
    mock_dag2.tags = []
    mock_dag2.next_dagrun_logical_date = None

    with patch("airflow_api_client.commands.list.DAGBag") as mock_dagbag:
        with patch("airflow_api_client.commands.list.output") as mock_output:
            mock_dagbag.return_value.get_all_dags.return_value = [mock_dag1, mock_dag2]

            cmd = ListDagsCommand()
            cmd._list_dags(mock_app)

            # Check that output was called with only 1 DAG (the filtered one)
            mock_output.assert_called_once()
            call_args = mock_output.call_args[0]
            data = call_args[0]  # First positional arg

            # For human output, data is a list of rows
            assert len(data) == 1  # Only paused DAG


def test_list_dag_runs_command(mock_app):
    """Test list dag-runs subcommand"""
    mock_app.args.list_resource = "dag-runs"
    mock_app.args.dag_id = "test_dag"
    mock_app.args.limit = 10

    # Mock DAG and DAGRuns
    mock_dag = MagicMock(spec=DAG)
    mock_dag.id = "test_dag"

    mock_run = MagicMock(spec=DAGRun)
    mock_run.id = "run_123"
    mock_run.state = "success"

    with patch("airflow_api_client.commands.list.DAG") as mock_dag_class:
        mock_dag_class.return_value = mock_dag
        mock_dag.dag_runs = [mock_run]

        cmd = ListDagsCommand()
        cmd._list_dag_runs(mock_app)

        mock_dag_class.assert_called_once_with(mock_app, "test_dag")


def test_list_task_instances_command(mock_app):
    """Test list task-instances subcommand"""
    mock_app.args.list_resource = "task-instances"
    mock_app.args.spec = "test_dag/run_123"

    # Mock ResourceSpec and task instances
    mock_spec = MagicMock()
    mock_ti = MagicMock()
    mock_ti.task_id = "task1"
    mock_ti.state = MagicMock(value="success")
    mock_ti.duration = 5.0
    mock_ti.try_number = 1
    mock_ti.start_date = "2026-01-01"
    mock_ti.end_date = "2026-01-01"

    mock_spec.task_instance = None
    mock_spec.dag_run = MagicMock()
    mock_spec.dag_run.task_instances = [mock_ti]

    with patch("airflow_api_client.commands.list.ResourceSpec") as mock_resource_spec:
        with patch("airflow_api_client.commands.list.output") as mock_output:
            mock_resource_spec.from_string.return_value = mock_spec

            cmd = ListDagsCommand()
            cmd._list_task_instances(mock_app)

            # Verify ResourceSpec was created correctly
            mock_resource_spec.from_string.assert_called_once_with(mock_app, "test_dag/run_123")

            # Verify output was called with 1 task instance
            mock_output.assert_called_once()
            call_args = mock_output.call_args[0]
            data = call_args[0]
            assert len(data) == 1  # One task instance


def test_list_command_dispatch(mock_app):
    """Test that execute dispatches to correct subcommand"""
    cmd = ListDagsCommand()

    # Test dags dispatch
    mock_app.args.list_resource = "dags"
    with patch.object(cmd, "_list_dags") as mock_list_dags:
        cmd.execute(mock_app)
        mock_list_dags.assert_called_once_with(mock_app)

    # Test dag-runs dispatch
    mock_app.args.list_resource = "dag-runs"
    with patch.object(cmd, "_list_dag_runs") as mock_list_runs:
        cmd.execute(mock_app)
        mock_list_runs.assert_called_once_with(mock_app)

    # Test task-instances dispatch
    mock_app.args.list_resource = "task-instances"
    with patch.object(cmd, "_list_task_instances") as mock_list_tasks:
        cmd.execute(mock_app)
        mock_list_tasks.assert_called_once_with(mock_app)
