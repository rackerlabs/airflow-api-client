#!/usr/bin/env python3

"""Airflow API Client"""

# This is a client for Airflow 3.1 and the RAW environment
import argparse
import configparser
import importlib
import inspect
import os
import sys
from functools import cached_property
from importlib.metadata import PackageNotFoundError, entry_points, version
from pathlib import Path
from typing import Dict, Type

import airflow_client.client
import requests

from airflow_api_client.auth import AirflowAPIClientAuthProvider
from airflow_api_client.commands import Command
from airflow_api_client.io import yaml
from airflow_api_client.log import logger, set_log_level
from airflow_api_client.typedefs import App

try:
    __version__ = version("airflow-api-client")
except PackageNotFoundError:
    __version__ = "unknown"


PAYLOADS_PATH = Path(__file__).parent / "payloads"


def get_available_providers() -> Dict[str, Type[AirflowAPIClientAuthProvider]]:
    """Get all available auth providers."""
    providers = {}
    eps = entry_points(group="airflow_api_client.auth")
    for ep in eps:
        providers[ep.name] = ep.load()
    providers["default"] = AirflowAPIClientAuthProvider
    return providers


def builtin_commands() -> Dict[str, Type[Command]]:
    """Discover and return all built-in command classes defined in the commands directory."""
    commands = {}
    commands_dir = Path(__file__).parent / "commands"

    # Check both .py files and directories (packages)
    for item in commands_dir.iterdir():
        if item.name.startswith("_") or item.name == "base.py":
            continue

        # Handle both files and directories
        if item.is_file() and item.suffix == ".py":
            module_name = item.stem
        elif item.is_dir() and (item / "__init__.py").exists():
            module_name = item.name
        else:
            continue

        module = importlib.import_module(f"airflow_api_client.commands.{module_name}")

        for _, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, Command) and obj is not Command:
                commands[obj.name] = obj

    return commands


def get_available_commands() -> Dict[str, Type[Command]]:
    """Get all available commands, including built-in and plugin commands."""
    commands = {}

    # Register built-in commands
    for cmd_name, cmd_class in builtin_commands().items():
        commands[cmd_name] = cmd_class

    # Discover plugin commands (optional)
    for ep in entry_points(group="airflow_api_client.commands"):
        logger.debug("Loading plugin command", command=ep.name, module=ep.module)
        commands[ep.name] = ep.load()

    return commands


class Client(airflow_client.client.ApiClient):
    """Custom API client that extends the generated ApiClient from airflow_client and integrates
    with the authentication provider."""

    def __init__(self, auth, args):
        self.args = args
        self.auth = auth
        client_configuration = airflow_client.client.Configuration(host=args.endpoint)
        client_configuration.debug = args.verbose >= 2
        auth.set_client_configuration(client_configuration)
        super().__init__(client_configuration)

    def call_api(self, *args, _request_timeout=None, **kwargs):
        """Override to inject default timeout from CLI args."""
        if _request_timeout is None:
            _request_timeout = self.args.timeout
        return super().call_api(*args, _request_timeout=_request_timeout, **kwargs)

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


def main(commands, args) -> None:
    """Main function that initializes the client and executes the specified command based on the
    command-line arguments. It first determines the authentication provider to use based on the
    command-line arguments and configuration, then initializes the API client with the chosen
    authentication provider. Finally, it executes the appropriate function based on the specified
    command (list, run, create-payload, or xcom) and handles the output accordingly.
    """

    cmd = commands[args.command]()

    client = None
    if not cmd.no_client:
        auth_class = (
            get_available_providers()[args.auth_provider]
            if args.auth_provider
            else AirflowAPIClientAuthProvider
        )
        logger.debug(
            "Using auth provider", provider=args.auth_provider, auth_class=auth_class.__name__
        )
        auth = auth_class(args)
        client = Client(auth, args)

    app = App(client, args)
    cmd.execute(app)


def parse_args(commands, providers):
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
        "--profile",
        "-P",
        type=str,
        default="default",
        help="Profile name to use from config file (default: default)",
    )

    bootstrap_args, _ = bootstrap.parse_known_args()

    config = configparser.RawConfigParser()

    config_path = bootstrap_args.config_file or default_config_path

    if config_path.is_file():
        config.read(config_path)

    parser = argparse.ArgumentParser(description="RAW client", allow_abbrev=False)
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
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
        "--profile",
        "-P",
        type=str,
        default="default",
        help="Profile name to use from config file (default: default)",
    )
    parser.add_argument(
        "--endpoint",
        "-e",
        type=str,
        default=os.getenv(
            "AIRFLOW_API_CLIENT_ENDPOINT",
            config.get(bootstrap_args.profile, "endpoint", fallback=None),
        ),
        help="Override the Airflow API endpoint (useful for testing against non-prod environments)",
    )

    parser.add_argument(
        "--timeout",
        "-T",
        type=int,
        default=os.getenv(
            "AIRFLOW_API_CLIENT_TIMEOUT",
            config.getint(bootstrap_args.profile, "timeout", fallback=60),
        ),
        help="Timeout for API requests in seconds (default: 60)",
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

    # Let commands add their own subparsers and arguments
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")
    for cmd_class in commands.values():
        cmd_parser = subparsers.add_parser(cmd_class.name, help=cmd_class.help)
        cmd_class.add_arguments(cmd_parser)

    # Let auth providers add their own arguments to the parser
    for provider_class in providers.values():
        provider_class.add_arguments(bootstrap_args.profile, parser, config)

    args = parser.parse_args()

    if args.no_verbose:
        args.verbose = 0

    set_log_level(args.verbose)

    if config_path.is_file():
        logger.debug("Loaded configuration from file", path=str(config_path))

    # If the command requires an API client and no endpoint is provided, show an error and exit
    # We need to allow for commands like `profile create` by not requiring one across the board
    if not args.endpoint and not getattr(commands.get(args.command), "no_client", False):
        logger.error(
            "Airflow API endpoint must be provided via --endpoint, config file, or"
            " AIRFLOW_API_CLIENT_ENDPOINT environment variable."
            f"Use '{sys.argv[0]} profile create' to interactively create a profile"
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
        commands = get_available_commands()
        args = parse_args(commands, providers)
        main(commands, args)
    except Exception as ex:  # pylint: disable=broad-except
        if args and args.verbose >= 1:
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
