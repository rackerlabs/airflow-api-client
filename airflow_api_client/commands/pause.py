# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Code related to the `pause` sub-command."""

from airflow_api_client.commands import Command
from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.models import DAG

DESCRIPTION = """
Command to pause a DAG. This command allows you to pause a DAG by its ID. When a DAG is paused, it
will not be scheduled to run until it is resumed. This can be useful for temporarily stopping a DAG
from running without deleting it or changing its schedule.
"""


class PauseDagCommand(Command):
    """Command to pause a DAG."""

    name = "pause"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument("dag_id", help="DAG ID to pause")

    def execute(self, **kwargs) -> None:
        """Pause a DAG"""
        logger.debug("Executing PauseDagCommand")

        dag = DAG(self.app, self.app.args.dag_id)
        response = dag.pause()
        logger.info(f"DAG '{self.app.args.dag_id}' has been paused")

        if self.app.args.output_format in ("json", "yaml"):
            output(response.to_dict(), self.app.args)
