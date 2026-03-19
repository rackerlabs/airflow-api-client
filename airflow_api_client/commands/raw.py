"""Code related to the `raw` sub-command."""

import json

from airflow_api_client.commands import Command
from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.typedefs import App

DESCRIPTION = """
Make a raw API request to Airflow. This command allows you to interact with the Airflow API
directly, bypassing any predefined commands or logic. You can specify the endpoint, method, and
payload for your request, giving you full control over the API interaction.
"""


class RawCommand(Command):
    """Command to make a raw API request."""

    name = "raw"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        """Add arguments for the `raw` command."""
        parser.add_argument(
            "method",
            type=str.upper,
            choices=["GET", "POST", "PUT", "DELETE", "PATCH"],
            help="The HTTP method to use for the request",
        )
        parser.add_argument(
            "stub_url",
            type=str,
            help="The API endpoint to call (e.g., /dags/{dag_id}/pause)",
        )
        parser.add_argument(
            "--payload",
            type=str,
            default=None,
            help="The JSON payload to send with the request (for POST and PUT methods)",
        )
        parser.add_argument(
            "--header",
            action="append",
            help=(
                "Custom headers to include in the request (can be specified multiple times, e.g.,"
                " --header 'Foo=Bar' --header 'Baz=Qux')"
            ),
        )

    def execute(self, app: App, **kwargs) -> None:
        """Run a raw API request to Airflow based on the provided arguments."""

        logger.debug("Executing RawCommand")

        endpoint = f"{app.args.endpoint}/{app.args.stub_url.lstrip('/')}"

        headers = {
            "Authorization": f"Bearer {app.client.auth.client_token.token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        headers.update(dict([h.split("=", maxsplit=1) for h in app.args.header or []]))
        try:
            payload = json.loads(app.args.payload) if app.args.payload else None
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON payload: {e}")
            return

        logger.debug(
            f"Making {app.args.method} request to {endpoint}",
            headers=headers,
            payload=payload,
            timeout=app.args.timeout,
        )

        response = app.client.rest_client.request(
            app.args.method,
            endpoint,
            body=payload,
            headers=headers,
            _request_timeout=app.args.timeout,
        )

        if app.args.output_format not in ["json", "yaml"]:
            # There's no pretty-printing arbitrary output for this command, so we default to JSON if
            # the user specified an unsupported format
            app.args.output_format = "json"

        logger.info(f"Received response from Airflow API. Status code: {response.status}")
        response_b = response.response.read()

        try:
            output(json.loads(response_b), app.args)
        except json.JSONDecodeError:
            logger.debug("Response is not valid JSON, printing raw response")
            output(response_b, app.args)
