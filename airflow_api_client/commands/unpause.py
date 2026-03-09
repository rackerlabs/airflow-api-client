"""Code related to the `unpause` sub-command."""

from airflow_api_client.commands import Command
from airflow_api_client.io import output
from airflow_api_client.log import logger
from airflow_api_client.models import DAG
from airflow_api_client.typedefs import App

DESCRIPTION = """
Command to unpause a DAG. This command allows you to unpause a DAG by its ID.
"""


class UnpauseDagCommand(Command):
    """Command to unpause a DAG."""

    name = "unpause"
    help = DESCRIPTION

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument("dag_id", help="DAG ID to unpause")

    def execute(self, app: App, **kwargs) -> None:
        """Unpause a DAG"""
        logger.debug("Executing UnpauseDagCommand")

        dag = DAG(app, app.args.dag_id)
        response = dag.unpause()
        logger.info(f"DAG '{app.args.dag_id}' has been unpaused.")

        if app.args.output_format in ("json", "yaml"):
            output(response.to_dict(), app.args)
