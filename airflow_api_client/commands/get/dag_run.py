"""Get DAG run details"""

from airflow_api_client.log import logger
from airflow_api_client.models import ResourceSpec
from airflow_api_client.typedefs import App


class GetDagRunCommand:
    """Helper class for getting DAG run details"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for dag-run"""
        run_parser = subparsers.add_parser("dag-run", help="Get DAG run details")
        run_parser.add_argument(
            "spec",
            metavar="DAG_ID/RUN_ID",
            help="DAG run specification in format: DAG_ID/RUN_ID",
        )

    @staticmethod
    def get_dag_run(app: App) -> dict:
        """Get DAG run details and return as dict"""
        spec = ResourceSpec.from_string(app, app.args.spec)
        logger.info(f"Retrieved details for DAG run '{spec.dag_run.id}'")
        return spec.dag_run.to_dict()
