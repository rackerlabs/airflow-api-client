"""Main get command with subcommand dispatch"""

import json

from airflow_api_client.commands import Command
from airflow_api_client.commands.get.dag import GetDagCommand
from airflow_api_client.commands.get.dag_run import GetDagRunCommand
from airflow_api_client.commands.get.log import GetLogCommand
from airflow_api_client.commands.get.task_instance import GetTaskInstanceCommand
from airflow_api_client.io import output
from airflow_api_client.log import logger

DESCRIPTION = """
Command to get details about a specific Airflow resource. This command retrieves full details
about a DAG, DAG run, or task instance by its identifier. Unlike the list command which returns
multiple resources, get returns a single resource with all available details.
"""


class GetCommand(Command):
    """Command to get details about a specific resource."""

    name = "get"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        subparsers = parser.add_subparsers(dest="resource_type", required=True)

        GetDagCommand.add_subparser(subparsers)
        GetDagRunCommand.add_subparser(subparsers)
        GetTaskInstanceCommand.add_subparser(subparsers)
        GetLogCommand.add_subparser(subparsers)

    def execute(self, **kwargs) -> None:
        """Get details about a specific resource"""
        logger.debug(f"Executing GetCommand for resource type: {self.app.args.resource_type}")

        result = None
        if self.app.args.resource_type == "dag":

            result = GetDagCommand.get_dag(self.app)
        elif self.app.args.resource_type == "dag-run":

            result = GetDagRunCommand.get_dag_run(self.app)
        elif self.app.args.resource_type == "task-instance":

            result = GetTaskInstanceCommand.get_task_instance(self.app)
        elif self.app.args.resource_type == "log":
            result = GetLogCommand.get_log(self.app)

        if result:
            # For human output, format as key-value table
            if self.app.args.output_format == "human":
                self._output_human(result)
            else:
                output(result, self.app.args)

    def _output_human(self, data: dict) -> None:
        """Output data in human-readable key-value format"""

        rows = []
        for key, value in sorted(data.items()):
            # Format complex values as JSON
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, indent=2, default=str)
            else:
                value_str = str(value)
            rows.append([key, value_str])

        output(rows, self.app.args, headers=["Field", "Value"])
