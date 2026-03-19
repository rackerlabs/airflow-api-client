"""Tests for get log command and Log model"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from airflow_client.client.exceptions import NotFoundException

from airflow_api_client.commands.get.log import GetLogCommand
from airflow_api_client.models import DAG, DAGRun, Log, TaskInstance

# pylint: disable=protected-access,duplicate-code


def _make_log_response(*entries):
    """Create a mock TaskInstancesLogResponse with the given entries."""
    mock_response = MagicMock()
    log_lines = []
    for timestamp, event in entries:
        line = MagicMock()
        line.timestamp = timestamp
        line.event = event
        log_lines.append(line)
    mock_response.to_dict.return_value = {"content": log_lines}
    return mock_response


class TestLogModel:
    """Tests for the Log model"""

    def test_log_lazy_load(self, mock_app):
        """Test that log data is lazily loaded from the API"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, try_number=1)

        mock_app.client._task_instance_api.get_log.assert_not_called()

        _ = log.log_data

        mock_app.client._task_instance_api.get_log.assert_called_once_with(
            dag_id="test_dag", dag_run_id="run_123", task_id="test_task", try_number=1
        )

    def test_log_cached(self, mock_app):
        """Test that log data is cached after first fetch"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, try_number=1)

        _ = log.log_data
        _ = log.log_data

        mock_app.client._task_instance_api.get_log.assert_called_once()

    def test_log_preloaded(self, mock_app):
        """Test that passing log_data skips API call"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")
        preloaded = MagicMock()

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, log_data=preloaded)

        assert log.log_data == preloaded
        mock_app.client._task_instance_api.get_log.assert_not_called()

    def test_log_iter_skips_group_lines(self, mock_app):
        """Test that ::group:: lines are filtered out during iteration"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        ts = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
        log_response = _make_log_response(
            (None, "::group::Log message source details"),
            (ts, "DAG bundles loaded: s3-dags"),
            (ts, "Task completed successfully"),
            (None, "::endgroup::"),
        )

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, log_data=log_response)

        entries = list(log)
        assert len(entries) == 2
        assert entries[0]["event"] == "DAG bundles loaded: s3-dags"
        assert entries[1]["event"] == "Task completed successfully"

    def test_log_iter_keeps_null_timestamp_non_group(self, mock_app):
        """Test that non-group lines with null timestamps are kept"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log_response = _make_log_response(
            (None, "Some log line without timestamp"),
        )

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, log_data=log_response)

        entries = list(log)
        assert len(entries) == 1
        assert entries[0]["timestamp"] is None
        assert entries[0]["event"] == "Some log line without timestamp"

    def test_log_try_number(self, mock_app):
        """Test that try_number is passed to the API"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log = Log(app=mock_app, dag=dag, dag_run=dag_run, task_instance=ti, try_number=3)
        _ = log.log_data

        mock_app.client._task_instance_api.get_log.assert_called_once_with(
            dag_id="test_dag", dag_run_id="run_123", task_id="test_task", try_number=3
        )


class TestTaskInstanceGetLog:
    """Tests for TaskInstance.get_log"""

    def test_get_log_default_try_number(self, mock_app, mock_task_instance):
        """Test get_log defaults to task's try_number"""
        mock_app.client._task_instance_api.get_task_instance.return_value = mock_task_instance
        mock_task_instance.try_number = 3

        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log = ti.get_log()
        assert log.try_number == 3

    def test_get_log_explicit_try_number(self, mock_app):
        """Test get_log with explicit try_number"""
        dag = DAG(mock_app, "test_dag")
        dag_run = DAGRun(mock_app, dag, "run_123")
        ti = TaskInstance(mock_app, dag, dag_run, "test_task")

        log = ti.get_log(try_number=2)
        assert log.try_number == 2


class TestGetLogCommand:
    """Tests for the get log CLI command"""

    def test_get_log_json_output(self, mock_app):
        """Test get log with JSON output returns dict of logs"""
        mock_app.args.output_format = "json"
        mock_app.args.try_number = 1
        mock_app.args.spec = "test_dag/run_123/test_task"

        ts = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
        log_response = _make_log_response(
            (ts, "Task started"),
            (ts, "Task completed"),
        )

        mock_ti = MagicMock()
        mock_ti.id = "test_task"
        mock_ti.try_number = 1
        mock_ti.get_log.return_value = Log(
            app=mock_app,
            dag=MagicMock(),
            dag_run=MagicMock(),
            task_instance=mock_ti,
            log_data=log_response,
        )

        mock_spec = MagicMock()
        mock_spec.task_instance = mock_ti
        mock_spec.dag.id = "test_dag"
        mock_spec.dag_run.id = "run_123"

        with patch(
            "airflow_api_client.commands.get.log.ResourceSpec.from_string", return_value=mock_spec
        ):
            result = GetLogCommand.get_log(mock_app)

        assert "test_task" in result
        assert len(result["test_task"]) == 2

    def test_get_log_not_found(self, mock_app):
        """Test get log handles NotFoundException"""
        mock_app.args.output_format = "json"
        mock_app.args.try_number = 1
        mock_app.args.spec = "test_dag/run_123/test_task"

        mock_ti = MagicMock()
        mock_ti.id = "test_task"
        mock_ti.try_number = 1
        mock_ti.get_log.side_effect = NotFoundException(status=404, reason="Not Found")

        mock_spec = MagicMock()
        mock_spec.task_instance = mock_ti
        mock_spec.dag.id = "test_dag"
        mock_spec.dag_run.id = "run_123"

        with patch(
            "airflow_api_client.commands.get.log.ResourceSpec.from_string", return_value=mock_spec
        ):
            result = GetLogCommand.get_log(mock_app)

        assert result == {}

    def test_get_log_all_tasks(self, mock_app):
        """Test get log without task_id gets logs for all tasks"""
        mock_app.args.output_format = "json"
        mock_app.args.try_number = 1
        mock_app.args.spec = "test_dag/run_123"

        ts = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
        log_response = _make_log_response((ts, "Log line"))

        mock_ti1 = MagicMock()
        mock_ti1.id = "task_1"
        mock_ti1.get_log.return_value = Log(
            app=mock_app,
            dag=MagicMock(),
            dag_run=MagicMock(),
            task_instance=mock_ti1,
            log_data=log_response,
        )

        mock_ti2 = MagicMock()
        mock_ti2.id = "task_2"
        mock_ti2.get_log.return_value = Log(
            app=mock_app,
            dag=MagicMock(),
            dag_run=MagicMock(),
            task_instance=mock_ti2,
            log_data=log_response,
        )

        mock_spec = MagicMock()
        mock_spec.task_instance = None
        mock_spec.dag.id = "test_dag"
        mock_spec.dag_run.id = "run_123"
        mock_spec.dag_run.task_instances = [mock_ti1, mock_ti2]

        with patch(
            "airflow_api_client.commands.get.log.ResourceSpec.from_string", return_value=mock_spec
        ):
            result = GetLogCommand.get_log(mock_app)

        assert "task_1" in result
        assert "task_2" in result

    def test_get_log_human_output(self, mock_app):
        """Test get log with human output returns None"""
        mock_app.args.output_format = "human"
        mock_app.args.try_number = 1
        mock_app.args.time_format = "%Y-%m-%dT%H:%M:%S"
        mock_app.args.spec = "test_dag/run_123/test_task"

        mock_ti = MagicMock()
        mock_ti.id = "test_task"
        mock_ti.try_number = 1

        mock_spec = MagicMock()
        mock_spec.task_instance = mock_ti
        mock_spec.dag.id = "test_dag"
        mock_spec.dag_run.id = "run_123"

        with (
            patch(
                "airflow_api_client.commands.get.log.ResourceSpec.from_string",
                return_value=mock_spec,
            ),
            patch("airflow_api_client.commands.get.log.output"),
        ):
            result = GetLogCommand.get_log(mock_app)

        assert result is None
        mock_ti.get_log.assert_called_once_with(1)
