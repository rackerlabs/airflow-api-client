"""Base class for CLI commands."""

from abc import ABC, abstractmethod
from argparse import ArgumentParser


class Command(ABC):
    """Base class for CLI commands."""

    no_client = False

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
    def execute(self, app, **kwargs):
        """Execute the command."""
