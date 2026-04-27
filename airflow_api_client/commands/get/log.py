# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Get task instance log"""

from airflow_client.client.exceptions import NotFoundException

from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.models import ResourceSpec
from airflow_api_client.typedefs import App


class GetLogCommand:
    """Helper class for getting the log for a task instance"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for task-instance"""
        task_parser = subparsers.add_parser("log", help="Get task instance log")
        task_parser.add_argument(
            "spec",
            metavar="DAG_ID/RUN_ID[/TASK_ID]",
            help="Task instance specification in format: DAG_ID/RUN_ID/TASK_ID",
        )
        task_parser.add_argument(
            "--try-number",
            type=int,
            default=1,
            help="Try number of the task instance log to retrieve (default: 1)",
        )

        task_parser.add_argument(
            "--time-format",
            default="%Y-%m-%dT%H:%M:%S",
            type=str,
            help=(
                "Time format for log timestamps (only applicable"
                " for human output, default: '%%Y-%%m-%%dT%%H:%%M:%%S')."
                " Supports strftime format codes. Only applies to human output, ignored for"
                " JSON/YAML output."
            ),
        )

    @staticmethod
    def get_log(app: App) -> dict:
        """Get task instance logs and return as dict"""
        spec = ResourceSpec.from_string(app, app.args.spec)

        task_instances = [spec.task_instance] if spec.task_instance else []
        if not task_instances:
            task_instances = spec.dag_run.task_instances

        logger.debug(
            f"Getting logs for task instances: {task_instances} in "
            f" DAG: {spec.dag.id} and DAG Run: {spec.dag_run.id}"
        )

        if app.args.output_format == "human":
            identifier = f"{spec.dag.id}/{spec.dag_run.id}"
            for task_instance in task_instances:
                if task_instance.try_number < app.args.try_number:
                    logger.warning(
                        f"Task instance {task_instance.id} has try number "
                        f"{task_instance.try_number} which is less than the requested "
                        f"try number {app.args.try_number}. Returning latest available log instead."
                    )
                output(
                    [[f"Log for Task Instance: {identifier}/{task_instance.id}"]],
                    app.args,
                    headers=[],
                )
                try:
                    task_instance.get_log(app.args.try_number).output(
                        time_format=app.args.time_format
                    )
                except NotFoundException:
                    logger.error(
                        f"Failed to get log for task instance: {task_instance.id} (not found)"
                        " Upstream failed or skipped?"
                    )
                    continue
            return None

        # json or yaml output
        logs = {}
        for task_instance in task_instances:
            try:
                log = task_instance.get_log(try_number=app.args.try_number)
                logs[task_instance.id] = list(log)
            except NotFoundException:
                logger.error(
                    f"Failed to get log for task instance: {task_instance.id} (not found)"
                    " Upstream failed or skipped"
                )
                continue

        return logs
