"""List DAG runs command"""

from airflow_api_client.filters import filter_obj
from airflow_api_client.io import output
from airflow_api_client.models import DAG
from airflow_api_client.typedefs import App


class ListDagRunsCommand:
    """Helper class for listing DAG runs"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for dag-runs"""
        runs_parser = subparsers.add_parser("dag-runs", help="List DAG runs")
        runs_parser.add_argument("dag_id", help="DAG ID")
        runs_parser.add_argument("--limit", type=int, default=10, help="Limit results")

    @staticmethod
    def list_dag_runs(app: App, add_extra_attrs_fn) -> None:
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

                add_extra_attrs_fn(app, run, headers, attrs)

                data.append(attrs)
            output(data, app.args, headers=headers)
