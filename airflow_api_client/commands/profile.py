"""Code related to the `profile` sub-command."""

import configparser
import os
import sys
from getpass import getpass
from pathlib import Path

from airflow_api_client.commands import Command
from airflow_api_client.log import logger
from airflow_api_client.typedefs import App

try:
    import keyring
except ImportError:
    keyring = None

DESCRIPTION = """
Manage configuration profiles. Profiles store connection details for Airflow instances
in the config file and optionally store passwords securely in the system keyring.
"""


class ProfileCommand(Command):
    """Command to manage configuration profiles."""

    name = "profile"
    help = DESCRIPTION
    no_client = True

    @classmethod
    def add_arguments(cls, parser) -> None:
        subparsers = parser.add_subparsers(dest="profile_action", required=True)

        delete_parser = subparsers.add_parser("delete", help="Delete an existing profile")
        delete_parser.add_argument(
            "profile_name",
            help=("Name of the profile to delete."),
        )
        delete_parser.add_argument(
            "--keep-keyring-password",
            action="store_true",
            help="Don't delete password from system keyring",
        )

        create_parser = subparsers.add_parser("create", help="Create a new minimal profile")
        create_parser.add_argument(
            "--profile-name",
            help=("Name of the profile to create. (default: 'default')"),
            default="default",
        )
        create_parser.add_argument("--endpoint", "-e", required=False, help="Airflow API endpoint")
        create_parser.add_argument("--username", "-u", help="Airflow username")
        create_parser.add_argument(
            "--password",
            "-p",
            help=(
                "Airflow password. If not provided, the password will be read from the"
                " AIRFLOW_API_CLIENT_PASSWORD environment variable, or prompted for interactively."
                " When keyring is available, the password is stored securely in the system keyring."
                " Use --no-keyring-password to store in plaintext in the config file instead."
            ),
        )
        create_parser.add_argument(
            "--no-keyring-password",
            action="store_true",
            help=(
                "Don't store password in system keyring, even if available (not recommended)."
                " Providing --no-keyring-password will store the password in plaintext in the"
                " config file,"
            ),
        )

    def execute(self, app: App, **kwargs) -> None:
        """Execute profile subcommand"""
        if app.args.profile_action == "create":
            self._create(app)

        elif app.args.profile_action == "delete":
            self._delete(app)

    def get_password(self, args, endpoint, username) -> str:
        """Get password from command line arguments or environment variable, or fall-back to
        prompting the user"""
        if args.password:
            return args.password

        env_password = os.getenv("AIRFLOW_API_CLIENT_PASSWORD")
        if env_password:
            return env_password

        if sys.stdin.isatty():
            return getpass(f"Enter Airflow password for {username} at {endpoint}: ")

        return sys.stdin.readline().strip()

    def _delete(self, app: App) -> None:
        """Delete an existing profile"""
        default_config_dir = os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))
        config_path = (
            app.args.config_file or Path(default_config_dir) / "airflow-api-client" / "config.ini"
        )
        if not config_path.is_file():
            raise RuntimeError(f"No config file found at {config_path}")

        config = configparser.RawConfigParser()
        config.read(config_path)
        profile = app.args.profile_name

        if not config.has_section(profile):
            raise RuntimeError(f"Profile '{profile}' not found in config file")

        if keyring and not app.args.keep_keyring_password:
            try:
                keyring.delete_password(
                    f"airflow-api-client/{config.get(profile, 'endpoint')}",
                    config.get(profile, "username"),
                )
                logger.info("Password removed from system keyring")
            except keyring.errors.PasswordDeleteError:
                logger.debug("No keyring entry found for this profile")

        config.remove_section(profile)
        with config_path.open("w", encoding="utf-8") as f:
            config.write(f)

        logger.info(f"Profile '{profile}' deleted from {config_path}")

    def _create(self, app: App) -> None:
        """Create a new profile"""
        default_config_dir = os.getenv("XDG_CONFIG_HOME", str(Path.home() / ".config"))
        config_path = (
            app.args.config_file or Path(default_config_dir) / "airflow-api-client" / "config.ini"
        )
        config_path.parent.mkdir(parents=True, exist_ok=True)

        config = configparser.RawConfigParser()
        if config_path.is_file():
            config.read(config_path)

        profile = app.args.profile_name

        if config.has_section(profile):
            logger.warning(f"Profile '{profile}' already exists, overwriting")
        else:
            config.add_section(profile)

        if (not app.args.endpoint or not app.args.username) and not sys.stdin.isatty():
            raise RuntimeError(
                "Endpoint and username must be provided as arguments when running in"
                " non-interactive mode. Use --endpoint and --username options or run the command in"
                " an interactive terminal."
            )

        endpoint = app.args.endpoint or input("Enter Airflow API endpoint: ")
        username = app.args.username or input("Enter Airflow username: ")

        endpoint = endpoint.strip().rstrip("/")
        config.set(profile, "endpoint", endpoint)
        config.set(profile, "username", username)

        password = self.get_password(app.args, endpoint, username)

        if keyring and not app.args.no_keyring_password:
            keyring.set_password(f"airflow-api-client/{endpoint}", username, password)
            logger.info("Password stored in system keyring")
        else:
            config.set(profile, "password", password)

        with config_path.open("w", encoding="utf-8") as f:
            config.write(f)

        logger.info(f"Profile '{profile}' saved to {config_path}")
