"""List task instances command"""

from airflow_api_client.filters import filter_obj
from airflow_api_client.io import output
from airflow_api_client.models import ResourceSpec
from airflow_api_client.typedefs import App


class ListTaskInstancesCommand:
    """Helper class for listing task instances"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for task-instances"""
        tasks_parser = subparsers.add_parser("task-instances", help="List task instances")
        tasks_parser.add_argument(
            "spec",
            metavar="DAG_ID/RUN_ID[/TASK_ID]",
            help=(
                "Retrieve information about the task instance(s) related to the  specified DAG run."
                " The format should be DAG_ID:DAG_RUN_ID  Optionally you can append :TASK_ID."
                " to retrieve a specific task instance."
            ),
        )

    @staticmethod
    def list_task_instances(app: App, add_extra_attrs_fn) -> None:
        """List task instances for a given DAG run"""

        spec = ResourceSpec.from_string(app, app.args.spec)

        task_instances = (
            [spec.task_instance] if spec.task_instance is not None else spec.dag_run.task_instances
        )
        task_instances = [t for t in task_instances if filter_obj(t, app.args.filter)]

        if app.args.output_format in ("json", "yaml"):
            output({"task_instances": [t.to_dict() for t in task_instances]}, app.args)
        else:
            headers = ["Task ID", "State", "Duration", "Try Number", "Start Date", "End Date"]
            data = []
            for ti in task_instances:
                attrs = [
                    ti.task_id,
                    ti.state.value,
                    ti.duration,
                    ti.try_number,
                    ti.start_date,
                    ti.end_date,
                ]

                add_extra_attrs_fn(app, ti, headers, attrs)

                data.append(attrs)
            output(data, app.args, headers=headers)
