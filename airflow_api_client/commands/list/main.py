# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Main list command with subcommand dispatch"""

from airflow_api_client.commands.list.base import ListCommandBase
from airflow_api_client.commands.list.dag_runs import ListDagRunsCommand
from airflow_api_client.commands.list.dags import ListDagsCommand
from airflow_api_client.commands.list.task_instances import ListTaskInstancesCommand
from airflow_api_client.log import logger

DESCRIPTION = """
Command to list various resources in Airflow, such as DAGs, DAG runs, and task instances.
Supports filtering and custom attribute selection for flexible querying.
"""


class ListCommand(ListCommandBase):
    """Command to list various resources in Airflow"""

    name = "list"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        cls.add_common_arguments(parser)

        subparsers = parser.add_subparsers(dest="list_resource", required=True)
        ListDagsCommand.add_subparser(subparsers)
        ListDagRunsCommand.add_subparser(subparsers)
        ListTaskInstancesCommand.add_subparser(subparsers)

    def execute(self, **kwargs) -> None:
        logger.debug("Executing ListCommand")

        if self.app.args.list_resource == "dags":
            ListDagsCommand.list_dags(self.app, self._add_extra_attrs)
        elif self.app.args.list_resource == "dag-runs":
            ListDagRunsCommand.list_dag_runs(self.app, self._add_extra_attrs)
        elif self.app.args.list_resource == "task-instances":
            ListTaskInstancesCommand.list_task_instances(self.app, self._add_extra_attrs)
