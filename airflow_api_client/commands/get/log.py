"""Get task instance log"""

import os
import shutil

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
                " Supports strftime format codes."
            ),
        )

    @staticmethod
    def get_log(app: App) -> dict:
        """Get task instance details and return as dict"""
        spec = ResourceSpec.from_string(app, app.args.spec)

        task_instances = [spec.task_instance.id] if spec.task_instance else []
        if not task_instances:
            task_instances = [ti.id for ti in spec.dag_run.task_instances]

        logger.debug(
            f"Getting logs for task instances: {task_instances} in "
            f" DAG: {spec.dag.id} and DAG Run: {spec.dag_run.id}"
        )

        logs = {}
        for task_instance in task_instances:
            logger.debug(
                f"Getting log for task instance: {task_instance} (try number:"
                f" {app.args.try_number})"
            )
            # pylint: disable=protected-access
            try:
                logs[task_instance] = {
                    "log": app.client._task_instance_api.get_log(
                        spec.dag.id,
                        spec.dag_run.id,
                        task_instance,
                        try_number=app.args.try_number,
                    ).content,
                }
            except NotFoundException:
                logger.debug(
                    f"Failed to get log for task instance: {task_instance} not found."
                    " Upstream failed or skipped"
                )
                continue

        # This is an outlier, we only let the main command code handle the output for json and
        # yaml formats. For human output, we handle the rendering here to provide a more
        # user-friendly display of the logs. Logs can be quite verbose and not lend itself
        # well to grid-display
        if app.args.output_format in ["json", "yaml"]:
            return logs

        # Human output, we iterate over the logs and returns their to_str() representation, which is
        # the raw log content. We also print the actual_instance and its type for debugging
        # purposes.

        for task_instance, log_info in logs.items():
            rendered = []
            for log_events in [li.to_dict() for li in log_info.values()]:
                for log_event in log_events:
                    event_time = (
                        log_event.timestamp.strftime(app.args.time_format)
                        if log_event.timestamp
                        else ""
                    )
                    rendered.append([event_time, log_event.event])
            logger.info(f"Log entries for task instance: {task_instance}")

            # We respect the environment variable, but if left up to us, we default to
            # "simple" for better readability as log lines tend to be a bit long
            tablefmt = os.getenv("AIRFLOW_API_CLIENT_TABLE_FORMAT", "simple")

            # Get the length of the timestamp to adjust the max column width for the log event
            # column. We subtract 30 to account for the timestamp and padding.
            padding = len(rendered[0][0]) + 24  # Timestamp length + padding
            maxcolwidth = shutil.get_terminal_size((80, 20)).columns - padding
            output(
                rendered,
                app.args,
                headers=["Timestamp", "Log Event"],
                tablefmt=tablefmt,
                maxcolwidths=[None, max(20, maxcolwidth)],
            )

        return None
