# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Get task instance details"""

from airflow_api_client.log import logger
from airflow_api_client.models import ResourceSpec
from airflow_api_client.typedefs import App


class GetTaskInstanceCommand:
    """Helper class for getting task instance details"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for task-instance"""
        task_parser = subparsers.add_parser("task-instance", help="Get task instance details")
        task_parser.add_argument(
            "spec",
            metavar="DAG_ID/RUN_ID/TASK_ID",
            help="Task instance specification in format: DAG_ID/RUN_ID/TASK_ID",
        )

    @staticmethod
    def get_task_instance(app: App) -> dict:
        """Get task instance details and return as dict"""
        spec = ResourceSpec.from_string(app, app.args.spec)
        if spec.task_instance is None:
            raise ValueError(
                f"Invalid task instance specification: {app.args.spec}. "
                "Expected format: DAG_ID/RUN_ID/TASK_ID"
            )
        logger.info(f"Retrieved details for task instance '{spec.task_instance.task_id}'")
        return spec.task_instance.to_dict()
