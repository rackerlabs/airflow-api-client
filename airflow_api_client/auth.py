import base64
import json
import os
import time
from abc import ABC, abstractproperty
from argparse import ArgumentParser
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    # The type hint for `args` argument
    from configparser import RawConfigParser
    from airflow_client.client import ApiClient


class AuthToken(ABC):
    @abstractproperty
    def expires(self) -> int:
        """Returns the number of seconds until the token expires."""


class AirflowClientJWTToken(AuthToken):
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
    def __init__(self, args):
        self.args = args
        self._token = None

    @property
    def password(self) -> str:
        if self.args.password is None:
            raise ValueError("Password is required for DefaultAuthProvider")
        return self.args.password

    @property
    def username(self) -> str:
        if self.args.username is None:
            raise ValueError("Username is required for DefaultAuthProvider")
        return self.args.username

    @property
    def client_token(self) -> AirflowClientJWTToken:
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
        # Default to JWT bearer token authentication
        client.access_token = self.client_token.token

    @classmethod
    def add_arguments(cls, profile: str, parser: "ArgumentParser", config: "RawConfigParser"):
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
