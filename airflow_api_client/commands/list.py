"""Code related to the xcom sub-command"""

from typing import Any, List

from airflow_api_client.commands import Command
from airflow_api_client.filters import filter_obj, get_obj_value
from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.models import DAG, DAGBag, ResourceSpec
from airflow_api_client.typedefs import App


class ListDagsCommand(Command):
    """Command to list various resources in Airflow"""

    name = "list"
    help = "List various resources in Airflow, such as DAGs, tasks, etc."

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument(
            "--filter",
            "-f",
            action="append",
            help="""
Filter results based on an object's value on the form: 'attr operator value', ie
'is_paused == true' or 'owners !^ "airflow"'. Supported operators:
" ==, !=, >, <, ^ (contains), !^ (not contains).  Repeat for multiple filters.
Note that the value will be serialised using JSON, so use types accordingly. (eg. set `value` to
`null` in order to match attributes with a value of `None` and enclose strings in
quotation marks etc. Supports jmespath-notation to traverse objects.
Note that this functionality is generic and can be used to filter on any
attribute of any underlying object's JSON representation (see --output-format json) for obtaining
the full JSON representation of each object to determine which attributes to filter on.
See the airflow_client Python library docs for more info.
            """,
        )
        parser.add_argument(
            "--extra-attrs",
            "-e",
            type=str,
            action="append",
            help="""
                Extra attribute to include in the output.
This will be included in the output alongside the default attributes for each resource. Output
may be modified to better suit tabular representation. Repeat for multiple extra attributes.
Related to notices in --filter. Richer objects such as lists and dictionaries are converted to
comma-separated strings for better readability in table output, for more complex or verbatim
output consider using the --output-format option with json or yaml and parsing the
output with jq/yq or similar. Supports jmespath-notation to select attributes
Example: list -e last_parse_duration -e owners -e default_args.has_on_failure_callback dags
Or see slow DAG Runs: list -f "duration > 10" dag-runs my_dag_id
                  """,
        )
        # Create subparsers for list subcommands
        subparsers = parser.add_subparsers(dest="list_resource", required=True)

        # list dags
        _ = subparsers.add_parser("dags", help="List all DAGs")

        # list dag-runs
        runs_parser = subparsers.add_parser("dag-runs", help="List DAG runs")
        runs_parser.add_argument("dag_id", help="DAG ID")
        runs_parser.add_argument("--limit", type=int, default=10, help="Limit results")

        # list task-instances
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

    def _add_extra_attrs(self, app: App, obj: Any, headers: List, attrs: List) -> None:
        """Helper method to add extra attributes to the output based on user input. Modifies headers
        and attrs lists in place.
        """
        for extra_attr in app.args.extra_attrs or []:
            extra_attr_value = get_obj_value(obj, extra_attr)
            # Make lists and dicts more readable in output by converting to comma-separated strings
            # Use keys only for dicts to avoid overly verbose output, for that kind of output, users
            # are expected to use the --output-format json/yaml and parse the output with jq/yq or
            # similar
            if hasattr(extra_attr_value, "__iter__") and not isinstance(extra_attr_value, str):
                extra_attr_value = ", ".join(str(v) for v in extra_attr_value)

            headers.append(extra_attr.replace("_", " ").replace(".", " ").title())
            attrs.append(extra_attr_value)

    def _list_dag_runs(self, app: App) -> None:
        """List DAG runs for a given DAG"""

        dag = DAG(app, app.args.dag_id)

        dag_runs = dag.get_dag_runs(app.args.limit)

        if app.args.filter:
            dag_runs = [d for d in dag_runs if filter_obj(d, app.args.filter)]

        if app.args.output_format in ("json", "yaml"):
            output({"dag_runs": [d.to_dict() for d in dag_runs]}, app.args)
        else:
            headers = [
                "DAG ID",
                "Run ID",
                "State",
                "Run Type",
                "Triggered By",
                "Duration",
                "Start Date",
                "End Date",
            ]
            data = []
            for run in dag_runs:
                attrs = [
                    run.dag_id,
                    run.id,
                    run.state.value,
                    run.run_type.value,
                    run.triggered_by.value,
                    run.duration,
                    run.start_date,
                    run.end_date,
                ]

                self._add_extra_attrs(app, run, headers, attrs)

                data.append(attrs)
            output(data, app.args, headers=headers)

    def _list_dags(self, app: App) -> None:
        """List all DAGs in Airflow"""

        # Use the lighter API call to get the list of DAGs unless extra details are needed for
        # filtering or output
        details = any(
            [app.args.extra_attrs, app.args.filter, app.args.output_format in ["yaml", "json"]]
        )

        all_dags = [
            dag
            for dag in DAGBag(app).get_all_dags(details=details)
            if filter_obj(dag, app.args.filter)
        ]

        if app.args.output_format in ("json", "yaml"):
            output({"dags": [d.to_dict() for d in all_dags]}, app.args)
            return

        headers = ["DAG ID", "Display Name", "Is Paused", "Owners", "Tags", "Next Dag Run"]
        data = []
        for dag in all_dags:
            attrs = [
                dag.dag_id,
                dag.dag_display_name,
                "Yes" if dag.is_paused else "No",
                ", ".join(dag.owners),
                ", ".join([t.name for t in dag.tags]),
                dag.next_dagrun_logical_date or "N/A",
            ]

            self._add_extra_attrs(app, dag, headers, attrs)

            data.append(attrs)

        output(data, app.args, headers=headers)

    def _list_task_instances(self, app: App) -> None:
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

                self._add_extra_attrs(app, ti, headers, attrs)

                data.append(attrs)
            output(data, app.args, headers=headers)

    def execute(self, app: App, **kwargs) -> None:
        logger.debug("Executing ListDagsCommand")

        if app.args.list_resource == "dags":
            self._list_dags(app)
        elif app.args.list_resource == "dag-runs":
            self._list_dag_runs(app)
        elif app.args.list_resource == "task-instances":
            self._list_task_instances(app)
