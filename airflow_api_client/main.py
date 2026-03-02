#!/usr/bin/env python3

"""Airflow API Client"""

# This is a client for Airflow 3.1 and the RAW environment
import argparse
import configparser
import json
import os
import sys
import time
from functools import cached_property
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import airflow_client.client
import requests
from tabulate import tabulate

from airflow_api_client.auth import AirflowAPIClientAuthProvider
from airflow_api_client.log import logger, set_log_level
from airflow_api_client.models import DAG, DAGBag, DAGRun, TaskInstance, XComEntry

try:
    import yaml
except ImportError:
    yaml = None

PAYLOADS_PATH = Path(__file__).parent / "payloads"


class App:  # pylint: disable=too-few-public-methods
    """Main application class that holds the API client and command-line arguments."""

    def __init__(self, client, args):
        self.client = client
        self.args = args


def output(data, args, headers=None):
    """Outputs data in the specified format (JSON, YAML, or human-readable table)."""
    if args.output_format == "json":
        print(json.dumps(data, indent=4, sort_keys=True, default=str))
    elif args.output_format == "yaml":

        print(yaml.dump(data, sort_keys=False))
    elif headers:  # human output, with tabular data
        print(
            tabulate(
                data,
                headers=headers,
                tablefmt=os.getenv("AIRFLOW_API_CLIENT_TABLE_FORMAT", "simple"),
            )
        )
    else:  # human output, raw/unformatted data
        print(data)


def get_available_providers():
    """Get all available auth providers."""
    providers = {}
    eps = entry_points(group="airflow_api_client.auth")
    for ep in eps:
        providers[ep.name] = ep.load()
    providers["default"] = AirflowAPIClientAuthProvider
    return providers


class Client(airflow_client.client.ApiClient):
    """Custom API client that extends the generated ApiClient from airflow_client and integrates
    with the authentication provider."""

    def __init__(self, auth, args):
        self.args = args
        self.auth = auth
        client_configuration = airflow_client.client.Configuration(host=args.endpoint)
        client_configuration.debug = args.verbose >= 3
        auth.set_client_configuration(client_configuration)
        super().__init__(client_configuration)

    def _make_raw_request(self, method, path, **kwargs):
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.auth.client_token}",
        }
        result = requests.request(
            method, f"{self.args.endpoint}/api/v2{path}", headers=headers, timeout=60, **kwargs
        )
        return result.json()

    # pylint: disable=invalid-name
    @cached_property
    def _DAG_api(self) -> airflow_client.client.DAGApi:
        return airflow_client.client.DAGApi(self)

    @cached_property
    def _DAG_run_api(self) -> airflow_client.client.DagRunApi:
        return airflow_client.client.DagRunApi(self)

    @cached_property
    def _xCom_api(self) -> airflow_client.client.XComApi:
        return airflow_client.client.XComApi(self)

    @cached_property
    def _task_instance_api(self) -> airflow_client.client.TaskInstanceApi:
        return airflow_client.client.TaskInstanceApi(self)

    # pylint: enable=invalid-name


def get_xcom(app: App, xcom_spec: str) -> Union[None, Dict[str, Any]]:
    """Retrieves XCom entries based on the provided specification. The xcom_spec should be in
    the format DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]. Depending on the level of specificity in the
    xcom_spec, it retrieves the corresponding XCom entries and formats the output
    """
    parts = xcom_spec.split(":")
    if len(parts) < 2:
        logger.error(
            "Invalid xcom specification format",
            expected_format="DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]",
        )
        raise ValueError(
            "Invalid xcom specification format. Expected DAG_ID:DAG_RUN_ID[:TASK_ID[:KEY]]"
        )
    dag_id, dag_run_id = parts[0], parts[1]
    task_id = parts[2] if len(parts) > 2 else None
    key = parts[3] if len(parts) > 3 else None

    logger.debug(
        "Getting XCom entry with specification",
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        task_id=task_id,
        key=key,
    )

    entries: List[XComEntry] = []

    if task_id:  # Covers dag_id:dag_run_id:task_id and dag_id:dag_run_id:task_id:key
        ti = TaskInstance(app, dag_id, dag_run_id, task_id)
        entries.extend(ti.get_xcom_entries(key=key))
    else:  # dag_id:dag_run_id
        entries = []
        dag_run = DAGRun(app, dag_id, dag_run_id)
        for ti in dag_run.task_instances:
            entries.extend(ti.get_xcom_entries(key=key))

    if not entries:
        logger.warning("No XCom entries found for the specified specification", xcom_spec=xcom_spec)
        return None

    entries.sort(key=lambda e: e.timestamp)
    if app.args.output_format in ("json", "yaml"):
        dagrun_output = {"dag_id": dag_id, "dag_run_id": dag_run_id}
        for entry in entries:
            dagrun_output.setdefault("tasks", {}).setdefault(entry.task_id, {})[entry.key] = {
                "value": entry.value,
                "timestamp": entry.timestamp,
            }
        output(dagrun_output, app.args)
        return dagrun_output

    # Standard human-readable output, sorted by timestamp
    for entry in entries:
        headers = ["Task ID", "Key", "Timestamp", "Value"]
        data = [
            [
                entry.task_id,
                entry.key,
                entry.timestamp,
                entry.value,
            ]
        ]
        output(data, app.args, headers=headers)
    return None


def _get_xcom(
    app: App,
    dag_id: str,
    dag_run_id: str,
    task_id: Union[str, None] = None,
    key: Union[str, None] = None,
) -> List[airflow_client.client.models.response_get_xcom_entry.ResponseGetXcomEntry]:
    # Retrieves the value of an XCom entry for a given DAG run and task. The xcom_spec should be
    logger.debug(
        "Retrieving XCom entry", dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, key=key
    )

    if task_id:  # Covers dag_id:dag_run_id:task_id and dag_id:dag_run_id:task_id:key
        ti = TaskInstance(app, dag_id, dag_run_id, task_id)
        return ti.get_xcom_entries(key=key)

    # dag_id:dag_run_id
    entries = []
    dag_run = DAGRun(app, dag_id, dag_run_id)
    for ti in dag_run.task_instances:
        entries.extend(ti.get_xcom_entries(key=key))
    return entries


def list_dags(app: App) -> Dict[str, Union[int, List[Dict[str, Any]]]]:
    """Lists all DAGs in the Airflow instance. Retrieves all DAGs using the DAGBag class and
    formats the output based on the specified output format (JSON, YAML, or human-readable table).
    """
    all_dags = DAGBag(app).get_all_dags(details=False)
    if app.args.output_format in ("json", "yaml"):
        output({"dags": [d.to_dict() for d in all_dags]}, app.args)
    else:
        headers = ["DAG ID", "Display Name", "Is Paused", "Owners", "Tags", "Next Dag Run"]
        data = []
        for dag in all_dags:
            data.append(
                [
                    dag.dag_id,
                    dag.dag_display_name,
                    "Yes" if dag.is_paused else "No",
                    ", ".join(dag.owners),
                    ", ".join([t.name for t in dag.tags]),
                    dag.next_dagrun_logical_date,
                ]
            )
        output(data, app.args, headers=headers)

    return {"dags": [d.to_dict() for d in all_dags]}


def get_payload_for_dag(app, dag_id, dag: Optional[Union[DAG, None]] = None):
    """Returns the payload expected for a given DAG. Prompts for values for each parameter
    defined in the DAG's parameters and constructs a payload dictionary.
    """
    dag = DAG(app, dag_id)
    parameters = dag.params or {}
    payload = {"conf": {}}
    logger.debug("Constructing payload for DAG run", dag_id=dag_id, parameters=parameters)

    for key, value in parameters.items():
        value_types = value["schema"]["type"]
        prompt = f"Enter value for parameter: '{key}'\n"
        prompt += f'Description: {value["description"]}'
        prompt += f'\nType(s): {", ".join(value_types)}'
        prompt += "\n> "
        sys.stdout.write(prompt)
        value = input()
        for value_type in value_types:
            if value_type == "null" and value == "":
                value = None
                break
            if value_type == "integer":
                value = int(value)
            elif value_type == "number":
                value = float(value)
            elif value_type == "boolean":
                value = value.lower() in ("true", "1", "yes")
        try:
            payload["conf"][key] = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            payload["conf"][key] = value
    return payload


def run_dag(app, dag_id, conf: Optional[Union[None, str, Dict]] = None) -> DAGRun:
    """Triggers a DAG with an optional payload. The response is printed to the console in the
    specified output format (JSON, YAML, or human-readable). If a payload is provided, it is used as
    the `conf` for the DAG run. If no payload is provided, the user is prompted to enter values for
    each parameter defined in the DAG's parameters. After triggering the DAG, the function polls for
    completion and retrieves any XCom entries associated with the DAG run.
    """

    conf = app.args.conf

    dag = DAG(app, dag_id)

    if conf is None:
        conf = get_payload_for_dag(app, dag_id, dag=dag)
    else:
        if conf.startswith("@"):
            conf_path = Path(conf[1:]).absolute()
            if not conf_path.is_file():
                logger.error("Conf file not found", path=str(conf_path))
                raise FileNotFoundError(f"Conf file not found: {conf_path}")
            with open(conf_path, "r", encoding="utf-8") as f:
                conf = f.read()
        conf = {"conf": json.loads(conf)}

    try:
        conf = json.loads(conf) if isinstance(conf, str) else conf
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON provided for --conf", error=str(e))
        raise

    dag_run: DAGRun = dag.run(conf=conf)

    # Poll for completion. Prefer this over calling airflow_client's wait_dag_run_until_finished
    # until that's no longer marked as experimental.
    while (current_state := dag_run.state) not in ("success", "failed"):
        logger.debug("Polling for DAG run completion", dag_id=dag_id, state=current_state)
        time.sleep(2)

    logger.info("DAG run completed", dag_id=dag_id, final_state=dag_run.state)

    get_xcom(app, f"{dag_id}:{dag_run.run_id}")

    return dag_run

    # self.get_xcom(f"{dag_id}:{dag_run_id}")


def main(args):
    """Main function that initializes the client and executes the specified command based on the
    command-line arguments. It first determines the authentication provider to use based on the
    command-line arguments and configuration, then initializes the API client with the chosen
    authentication provider. Finally, it executes the appropriate function based on the specified
    command (list, run, create-payload, or xcom) and handles the output accordingly.
    """
    auth_class = (
        get_available_providers()[args.auth_provider]
        if args.auth_provider
        else AirflowAPIClientAuthProvider
    )
    logger.debug("Using auth provider", provider=args.auth_provider, auth_class=auth_class.__name__)
    auth = auth_class(args)

    app = App(Client(auth, args), args)

    if args.command == "list":
        list_dags(app)
    elif args.command == "run":
        run_dag(app, args.dag_id, conf=args.conf)
    elif args.command == "create-payload":
        payload = get_payload_for_dag(app, args.dag_id)
        output(json.dumps(payload, indent=4, sort_keys=True, default=str), args)
    elif args.command == "xcom":
        get_xcom(app, args.xcom_spec)
    return True


def parse_args(providers):
    """Initial argument parsing to get profile and config file for loading potential configuration
    file before the main argument parsing, allowing config file values to be used as defaults in
    the main parser.
    """

    default_config_dir = os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))
    default_config_path = Path(default_config_dir) / "airflow-api-client" / "config.ini"

    bootstrap = argparse.ArgumentParser(add_help=False, allow_abbrev=False)
    bootstrap.add_argument(
        "--config-file",
        "-C",
        type=Path,
        help=f"Path to config file (default: {default_config_path})",
    )
    bootstrap.add_argument(
        "--profile", "-P", type=str, help="Profile name to use from config file (default: default)"
    )

    bootstrap_args, _ = bootstrap.parse_known_args()

    config = configparser.RawConfigParser()

    config_path = bootstrap_args.config_file or default_config_path

    if config_path.is_file():
        config.read(config_path)

    parser = argparse.ArgumentParser(description="RAW client", allow_abbrev=False)
    parser.add_argument(
        "--auth-provider",
        type=str,
        choices=get_available_providers().keys(),
        default=config.get(bootstrap_args.profile, "auth_provider", fallback="default"),
        help="Auth provider to use (default: None (standard JWT token with username/password)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_const",
        const=1,
        default=config.getint(bootstrap_args.profile, "verbose", fallback=0),
        help=("Enable verbose logging and output"),
    )
    parser.add_argument(
        "-vv",
        action="store_const",
        const=2,
        dest="verbose",
        help=(
            "More verbose logging (NOTE: May include sensitive information like passwords in"
            " output use with caution)"
        ),
    )
    parser.add_argument(
        "-vvv",
        action="store_const",
        const=3,
        dest="verbose",
        help=(
            "Enable or disable verbose logging (NOTE: WILL include sensitive information like"
            " passwords in output, use with caution)"
        ),
    )
    parser.add_argument(
        "--no-verbose",
        action="store_true",
        help="Disable verbose logging (override config)",
    )
    parser.add_argument(
        "--config-file",
        "-C",
        type=Path,
        help=f"Path to config file (default: {default_config_path})",
    )
    parser.add_argument(
        "--profile", "-P", type=str, help="Profile name to use from config file (default: default)"
    )
    parser.add_argument(
        "--endpoint",
        "-e",
        type=str,
        default=config.get(
            bootstrap_args.profile,
            "endpoint",
            fallback=os.getenv("AIRFLOW_API_CLIENT_ENDPOINT"),
        ),
        help="Override the Airflow API endpoint (useful for testing against non-prod environments)",
    )

    output_choices = ["human", "json"]
    if yaml:
        output_choices.append("yaml")

    parser.add_argument(
        "--output-format",
        "-f",
        choices=output_choices,
        default="human",
        help="Output format for commands that return data",
    )

    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # xcom subcommand
    xcom_parser = subparsers.add_parser("xcom", help="Get XCom entries")
    xcom_parser.add_argument(
        "xcom_spec",
        metavar="DAG_ID:RUN_ID[:TASK_ID[:KEY]]",
        help=(
            "Retrieve the value of the 'return_value' XCom for the specified DAG run."
            " The format should be DAG_ID:DAG_RUN_ID:TASK_ID. This is useful for retrieving results"
            " from a DAG run after it has completed. If the specified DAG run or task does not"
            "exist. Only DAG_ID:DAG_RUN_ID is required, if TASK_ID and KEY are omitted, the client"
            "will attempt to retrieve all xcom entries for all tasks. If only KEY is omitted, it"
            " will attempt to retrieve all xcom entries for the specified task."
        ),
    )

    # create-payload subcommand

    subparsers.add_parser("list", help="List DAGs")
    run_parser = subparsers.add_parser("run", help="Run a DAG")
    run_parser.add_argument("dag_id", help="DAG ID to run")
    run_parser.add_argument("--conf", "-c", type=str, help="JSON conf or @file")

    payload_parser = subparsers.add_parser(
        "create-payload",
        help=(
            "Create a payload for the specified DAG and exit. The payload will be printed"
            " to stderr the console (for easy redirection to a file if desired)"
        ),
    )
    payload_parser.add_argument("dag_id", help="DAG ID")

    # Let auth providers add their own arguments to the parser
    for provider_class in providers.values():
        provider_class.add_arguments(bootstrap_args.profile, parser, config)

    args = parser.parse_args()

    if args.no_verbose:
        args.verbose = 0

    set_log_level(args.verbose)

    if config_path.is_file():
        logger.debug("Loaded configuration from file", path=str(config_path))

    if not args.endpoint:
        logger.error(
            "Airflow API endpoint must be provided via --endpoint, config file, or"
            " AIRFLOW_API_CLIENT_ENDPOINT environment variable"
        )
        parser.print_help()
        sys.exit(1)

    return args


def cli():
    """Command-line interface entry point. Parses command-line arguments, initializes the API
    client, and executes the specified command. It also includes error handling to provide
    user-friendly error messages and logging based on the verbosity level specified in the
    command-line arguments.
    """
    args = None
    try:
        providers = get_available_providers()
        args = parse_args(providers)
        main(args)
    except Exception as ex:  # pylint: disable=broad-except
        if args and args.verbose >= 2:
            logger.exception("An error occurred", error=str(ex))
            sys.exit(1)

        # For non-verbose output, just print the error message without the stack trace
        # Handle airflow_client exceptions with more user-friendly messages
        if isinstance(ex, requests.exceptions.RequestException):
            logger.error(
                "Network error occurred while connecting to Airflow API",
                error=str(ex),
            )
        elif isinstance(ex, airflow_client.client.ApiException):
            logger.error(
                "API error occurred while communicating with Airflow API",
                status=ex.status,
                reason=ex.reason,
                body=ex.body,
            )
        else:
            logger.error("An error occurred", error=str(ex))

        sys.exit(1)


if __name__ == "__main__":
    cli()
