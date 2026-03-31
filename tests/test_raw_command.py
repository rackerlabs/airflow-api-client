"""Tests for raw command"""

import json
from unittest.mock import MagicMock, patch

from airflow_api_client.commands.raw import RawCommand

# pylint: disable=protected-access,duplicate-code


def _mock_response(body, status=200):
    """Create a mock REST response"""
    resp = MagicMock()
    resp.status = status
    resp.response.read.return_value = json.dumps(body).encode() if isinstance(body, dict) else body
    return resp


def test_raw_get_request(mock_app):
    """Test a basic GET request"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "/api/v2/dags"
    mock_app.args.payload = None
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({"dags": []})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output") as mock_output:
        cmd.execute()

    mock_app.client.rest_client.request.assert_called_once_with(
        "GET",
        "https://airflow.example.com/api/v2/dags",
        body=None,
        headers={
            "Authorization": "Bearer test-token",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        _request_timeout=30,
    )
    mock_output.assert_called_once_with({"dags": []}, mock_app.args)


def test_raw_post_with_payload(mock_app):
    """Test POST request with JSON payload"""
    mock_app.args.method = "POST"
    mock_app.args.stub_url = "/api/v2/dags/test_dag/dagRuns"
    mock_app.args.payload = '{"dag_run_id": "test_run", "conf": {}}'
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({"dag_run_id": "test_run"})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output"):
        cmd.execute()

    call_kwargs = mock_app.client.rest_client.request.call_args
    assert call_kwargs[1]["body"] == {"dag_run_id": "test_run", "conf": {}}


def test_raw_custom_headers(mock_app):
    """Test that custom headers are merged"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "/api/v2/dags"
    mock_app.args.payload = None
    mock_app.args.header = ["X-Custom=hello", "X-Other=world"]
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({"dags": []})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output"):
        cmd.execute()

    call_kwargs = mock_app.client.rest_client.request.call_args
    headers = call_kwargs[1]["headers"]
    assert headers["X-Custom"] == "hello"
    assert headers["X-Other"] == "world"


def test_raw_header_with_equals_in_value(mock_app):
    """Test header value containing = sign"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "/api/v2/dags"
    mock_app.args.payload = None
    mock_app.args.header = ["Authorization=Bearer abc=def="]
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output"):
        cmd.execute()

    call_kwargs = mock_app.client.rest_client.request.call_args
    assert call_kwargs[1]["headers"]["Authorization"] == "Bearer abc=def="


def test_raw_invalid_payload(mock_app):
    """Test that invalid JSON payload is handled gracefully"""
    mock_app.args.method = "POST"
    mock_app.args.stub_url = "/api/v2/dags"
    mock_app.args.payload = "not valid json"
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    cmd = RawCommand()
    cmd.app = mock_app
    cmd.execute()

    mock_app.client.rest_client.request.assert_not_called()


def test_raw_non_json_response(mock_app):
    """Test handling of non-JSON response body"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "/api/v2/health"
    mock_app.args.payload = None
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response(b"OK", status=200)

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output") as mock_output:
        cmd.execute()

    mock_output.assert_called_once_with(b"OK", mock_app.args)


def test_raw_human_format_defaults_to_json(mock_app):
    """Test that human output format is overridden to json"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "/api/v2/dags"
    mock_app.args.payload = None
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "human"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({"dags": []})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output"):
        cmd.execute()

    assert mock_app.args.output_format == "json"


def test_raw_strips_leading_slash(mock_app):
    """Test that leading slashes are normalized"""
    mock_app.args.method = "GET"
    mock_app.args.stub_url = "///api/v2/dags"
    mock_app.args.payload = None
    mock_app.args.header = None
    mock_app.args.endpoint = "https://airflow.example.com"
    mock_app.args.timeout = 30
    mock_app.args.output_format = "json"
    mock_app.client.auth.client_token.token = "test-token"

    mock_app.client.rest_client.request.return_value = _mock_response({})

    cmd = RawCommand()
    cmd.app = mock_app
    with patch("airflow_api_client.commands.raw.output"):
        cmd.execute()

    call_kwargs = mock_app.client.rest_client.request.call_args
    assert call_kwargs[1] is None or "///" not in call_kwargs[0][1]
