"""Tests for argument parsing"""

import os
from unittest.mock import patch

import pytest

from airflow_api_client.main import get_available_commands, get_available_providers, parse_args

# pylint: disable=protected-access,duplicate-code,redefined-outer-name


@pytest.fixture
def mock_config_file(tmp_path):
    """Create a temporary config file for testing"""
    config_file = tmp_path / "config.ini"
    config_file.write_text("""[default]
endpoint = https://config.example.com
username = config_user
password = config_pass
verbose = 1

[production]
endpoint = https://prod.example.com
username = prod_user
""")
    return config_file


@pytest.fixture
def commands():
    """Get available commands"""
    return get_available_commands()


@pytest.fixture
def providers():
    """Get available providers"""
    return get_available_providers()


def test_parse_args_cli_precedence(commands, providers, mock_config_file):
    """Test that CLI arguments take precedence over config and env"""
    with patch.dict(os.environ, {"AIRFLOW_API_CLIENT_ENDPOINT": "https://env.example.com"}):
        with patch(
            "sys.argv",
            [
                "airflow-api-client",
                "-C",
                str(mock_config_file),
                "-e",
                "https://cli.example.com",
                "list",
                "dags",
            ],
        ):
            args = parse_args(commands, providers)
            assert args.endpoint == "https://cli.example.com"


def test_parse_args_env_precedence(commands, providers, mock_config_file):
    """Test that env variables take precedence over config"""
    with patch.dict(os.environ, {"AIRFLOW_API_CLIENT_ENDPOINT": "https://env.example.com"}):
        with patch("sys.argv", ["airflow-api-client", "-C", str(mock_config_file), "list", "dags"]):
            args = parse_args(commands, providers)
            assert args.endpoint == "https://env.example.com"


def test_parse_args_config_fallback(commands, providers, mock_config_file):
    """Test that config is used when CLI and env are not set"""
    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["airflow-api-client", "-C", str(mock_config_file), "list", "dags"]):
            args = parse_args(commands, providers)
            assert args.endpoint == "https://config.example.com"


def test_parse_args_profile(commands, providers, mock_config_file):
    """Test that profile selection works"""
    with patch.dict(os.environ, {}, clear=True):
        with patch(
            "sys.argv",
            ["airflow-api-client", "-C", str(mock_config_file), "-P", "production", "list", "dags"],
        ):
            args = parse_args(commands, providers)
            assert args.endpoint == "https://prod.example.com"
            assert args.profile == "production"


def test_parse_args_verbose_levels(commands, providers, mock_config_file):
    """Test verbose flag levels"""
    with patch(
        "sys.argv", ["airflow-api-client", "-C", str(mock_config_file), "-v", "list", "dags"]
    ):
        args = parse_args(commands, providers)
        assert args.verbose == 1

    with patch(
        "sys.argv", ["airflow-api-client", "-C", str(mock_config_file), "-vv", "list", "dags"]
    ):
        args = parse_args(commands, providers)
        assert args.verbose == 2

    with patch(
        "sys.argv", ["airflow-api-client", "-C", str(mock_config_file), "-vvv", "list", "dags"]
    ):
        args = parse_args(commands, providers)
        assert args.verbose == 3


def test_parse_args_no_verbose_override(commands, providers, mock_config_file):
    """Test that --no-verbose overrides config"""
    with patch(
        "sys.argv",
        ["airflow-api-client", "-C", str(mock_config_file), "--no-verbose", "list", "dags"],
    ):
        args = parse_args(commands, providers)
        assert args.verbose == 0


def test_parse_args_xcom_command(commands, providers, mock_config_file):
    """Test xcom command parsing"""
    with patch(
        "sys.argv",
        [
            "airflow-api-client",
            "-C",
            str(mock_config_file),
            "xcom",
            "dag_id:run_id:task_id:key",
        ],
    ):
        args = parse_args(commands, providers)
        assert args.command == "xcom"
        assert args.xcom_spec == "dag_id:run_id:task_id:key"


def test_parse_args_run_command(commands, providers, mock_config_file):
    """Test run command parsing"""
    with patch(
        "sys.argv",
        [
            "airflow-api-client",
            "-C",
            str(mock_config_file),
            "run",
            "my_dag",
            "--conf",
            '{"key": "value"}',
        ],
    ):
        args = parse_args(commands, providers)
        assert args.command == "run"
        assert args.dag_id == "my_dag"
        assert args.conf == '{"key": "value"}'


def test_parse_args_output_format(commands, providers, mock_config_file):
    """Test output format selection"""
    with patch(
        "sys.argv",
        ["airflow-api-client", "-C", str(mock_config_file), "-f", "json", "list", "dags"],
    ):
        args = parse_args(commands, providers)
        assert args.output_format == "json"


def test_parse_args_missing_endpoint_exits(commands, providers, tmp_path):
    """Test that missing endpoint causes exit"""
    empty_config = tmp_path / "empty.ini"
    empty_config.write_text("[default]\n")

    with patch.dict(os.environ, {}, clear=True):
        with patch("sys.argv", ["airflow-api-client", "-C", str(empty_config), "list", "dags"]):
            with pytest.raises(SystemExit) as exc_info:
                parse_args(commands, providers)
            assert exc_info.value.code == 1
