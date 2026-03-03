"""Code related to the xcom sub-command"""

from airflow_api_client.commands import Command
from airflow_api_client.filters import filter_obj, get_obj_value, validate_filters
from airflow_api_client.log import logger
from airflow_api_client.models import DAGBag
from airflow_api_client.output import output
from airflow_api_client.typedefs import App


class ListDagsCommand(Command):
    """Command to list various resources in Airflow"""

    name = "list"
    help = "List various resources in Airflow, such as DAGs, tasks, etc."

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument(
            "--filter",
            "-F",
            action="append",
            help=(
                "Filter results based on an object's value on the form: 'attr operator value', ie"
                " 'is_paused==true' or 'owners==[\"ops team\"]'. Supported operators:"
                " ==, !=, >, <, ^ (contains)  Repeat for multiple filters. Note that the value"
                " will be serialised using json, so use types accordingly. (eg. set `value` to"
                " `null` in order to match attributes with a value of `None` and enclose strings in"
                " quotation marks etc. Supports dot-notation to traverse multiple objects."
                " Supports attributes that are callables (so long as they don't require arguments)."
                " Note that this functionality is generic and can be used to filter on any"
                " attribute of any underlying object being listed and therefore requires some"
                " knowledge of the underlying objects and their attributes to use effectively."
                " See the airflow_client Python library docs for more info"
            ),
        )
        parser.add_argument(
            "--extra-attrs",
            "-e",
            type=str,
            action="append",
            help="""
                Extra attribute to include in the output.
This will be included in the output alongside the default attributes for each" resource.
Note that this attribute will be included as-is in the output. Repeat for multiple extra attributes.
            Example: -e last_parse_duration -e owners
                  """,
        )
        # Create subparsers for list subcommands
        subparsers = parser.add_subparsers(dest="list_resource", required=True)

        # list dags
        _ = subparsers.add_parser("dags", help="List all DAGs")

    def _list_dags(self, app: App) -> None:
        """List all DAGs in Airflow"""

        all_dags = DAGBag(app).get_all_dags(details=False)
        print(dir(all_dags[0].dag))

        if app.args.filter:
            all_dags = [d for d in all_dags if filter_obj(d, app.args.filter)]

        if app.args.output_format in ("json", "yaml"):
            output({"dags": [d.to_dict() for d in all_dags]}, app.args)
        else:
            headers = ["DAG ID", "Display Name", "Is Paused", "Owners", "Tags", "Next Dag Run"]
            data = []
            for dag in all_dags:
                attrs = [
                    dag.dag_id,
                    dag.dag_display_name,
                    "Yes" if dag.is_paused else "No",
                    ", ".join(dag.owners),
                    ", ".join([t.name for t in dag.tags]),
                    dag.next_dagrun_logical_date,
                ]

                for extra_attr in app.args.extra_attrs or []:
                    extra_attr = extra_attr.strip()
                    headers.append(extra_attr.replace("_", " ").title())
                    extra_attr_value = get_obj_value(dag, extra_attr)
                    if hasattr(extra_attr_value, "__iter__") and not isinstance(
                        extra_attr_value, str
                    ):
                        extra_attr_value = ", ".join(str(v) for v in extra_attr_value)
                    attrs.append(extra_attr_value)
                data.append(attrs)
            output(data, app.args, headers=headers)
        return {"dags": [d.to_dict() for d in all_dags]}

    def execute(self, app: App, **kwargs) -> None:
        logger.debug("Executing ListDagsCommand")

        validate_filters(app.args.filter or [])

        if app.args.list_resource == "dags":
            self._list_dags(app)
