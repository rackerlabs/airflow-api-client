"""Authentication provider for Airflow API client that supports JWT tokens.
AirflowAPIClientAuthProvider is the default authentication provider for Airflow API client. Custom
providers can be implemented by subclassing AirflowAPIClientAuthProvider and overriding the
relevant methods. The provider can be configured via command-line arguments or environment
variables.

See --auth-provider argument in the CLI for more details on how to specify a custom authentication
provider.
"""

import base64
import json
import os
import time
from abc import ABC, abstractmethod
from argparse import ArgumentParser
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    # The type hint for `args` argument
    from configparser import RawConfigParser
    from airflow_client.client import ApiClient


class AuthToken(ABC):  # pylint: disable=too-few-public-methods
    """Abstract base class for authentication tokens."""
    @property
    @abstractmethod
    def expires(self) -> int:
        """Returns the number of seconds until the token expires."""


class AirflowClientJWTToken(AuthToken):  # pylint: disable=too-few-public-methods
    """Represents a JWT token used for authenticating with the Airflow API."""
    def __init__(self, token: str):
        self.token = token

        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError("Invalid JWT format")

        payload = parts[1]

        # Add padding if needed (JWT uses base64url without padding)
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += "=" * padding

        # Decode base64
        self.decoded = json.loads(base64.urlsafe_b64decode(payload))

    @property
    def expires(self) -> int:
        # Returns the number of seconds until the token expires
        return self.decoded.get("exp", 0) - int(time.time())


class AirflowAPIClientAuthProvider:
    """Default authentication provider for Airflow API client that supports username/password auth
    and JWT tokens."""
    def __init__(self, args):
        self.args = args
        self._token = None

    @property
    def password(self) -> str:
        """Returns the password for authentication. Raises an error if password is not provided."""
        if self.args.password is None:
            raise ValueError("Password is required for DefaultAuthProvider")
        return self.args.password

    @property
    def username(self) -> str:
        """Returns the username for authentication. Raises an error if username is not provided."""
        if self.args.username is None:
            raise ValueError("Username is required for DefaultAuthProvider")
        return self.args.username

    @property
    def client_token(self) -> AirflowClientJWTToken:
        """Returns a valid JWT token for authenticating with the Airflow API. If a valid token is
        already cached, it returns the cached token. Otherwise, it fetches a new token using the
        username and password."""
        if self._token and self._token.expires > 60:
            return self._token

        env_token = os.getenv("AIRFLOW_API_CLIENT_TOKEN")
        if env_token:
            return AirflowClientJWTToken(env_token)

        url = f"{self.args.endpoint}/auth/token"
        response = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={
                "username": self.username,
                "password": self.password,
            },
            timeout=10,
        )
        return AirflowClientJWTToken(response.json()["access_token"])

    def set_client_configuration(self, client: "ApiClient") -> None:
        """Configures the Airflow API client with the appropriate authentication headers. This
        method is called by the CLI when initializing the API client.
        `client` is the instance of airflow_client.client.ApiClient that needs to be configured with
        authentication headers.
        """
        client.access_token = self.client_token.token

    @classmethod
    def add_arguments(cls, profile: str, parser: "ArgumentParser", config: "RawConfigParser"):
        """Adds authentication-related command-line arguments to the parser. This method is called
        by the CLI when setting up the argument parser for the specified profile. The arguments
        added by this method will be available in the `args` object passed to the constructor of the
        authentication provider.
        """
        parser.add_argument(
            "--username",
            "-u",
            type=str,
            default=config.get(profile, "username", fallback=None),
            help="Airflow username (typically your SSO)",
        )
        parser.add_argument(
            "--password",
            "-p",
            type=str,
            default=config.get(profile, "password", fallback=None),
            help="Airflow password (if omitted, attempt to fetch from PWSafe",
        )
