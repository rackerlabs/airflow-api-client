# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Base class for CLI commands."""

from abc import ABC, abstractmethod
from argparse import ArgumentParser


class Command(ABC):
    """Base class for CLI commands."""

    no_client = False

    def __init__(self):
        # The app is set by the CLI in main() before executing the command.
        # This allows for commands that don't or can't require a client (eg. profile creation) to
        # still be executed.
        self.app = None

    @property
    @abstractmethod
    def name(self) -> str:
        """Command name (e.g., 'xcom', 'run')."""

    @property
    @abstractmethod
    def help(self) -> str:
        """Help text for the command."""

    @classmethod
    @abstractmethod
    def add_arguments(cls, parser: ArgumentParser):
        """Add command-specific arguments to the subparser."""

    @abstractmethod
    def execute(self, **kwargs):
        """Execute the command."""
