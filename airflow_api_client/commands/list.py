"""Code related to the xcom sub-command"""

from airflow_api_client.commands import Command
from airflow_api_client.log import logger
from airflow_api_client.models import DAGBag
from airflow_api_client.output import output
from airflow_api_client.typedefs import App


class ListDagsCommand(Command):
    """Command to retrieve XCom entries"""

    name = "list"
    help = "List all DAGs"

    @classmethod
    def add_arguments(cls, parser) -> None:
        pass

    def execute(self, app: App, **kwargs) -> None:
        logger.debug("Executing ListDagsCommand")
        all_dags = DAGBag(app).get_all_dags(details=False)
        if app.args.output_format in ("json", "yaml"):
            output({"dags": [d.to_dict() for d in all_dags]}, app.args)
        else:
            headers = ["DAG ID", "Display Name", "Is Paused", "Owners", "Tags", "Next Dag Run"]
            data = []
            for dag in all_dags:
                data.append(
                    [
                        dag.dag_id,
                        dag.dag_display_name,
                        "Yes" if dag.is_paused else "No",
                        ", ".join(dag.owners),
                        ", ".join([t.name for t in dag.tags]),
                        dag.next_dagrun_logical_date,
                    ]
                )
            output(data, app.args, headers=headers)

        return {"dags": [d.to_dict() for d in all_dags]}
