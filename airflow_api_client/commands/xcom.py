"""Code related to the xcom sub-command"""

from airflow_api_client.commands import Command
from airflow_api_client.log import logger
from airflow_api_client.models import ResourceSpec


class XComCommand(Command):
    """Command to retrieve XCom entries"""

    name = "xcom"
    help = "Retrieve XCom entries"

    @classmethod
    def add_arguments(cls, parser) -> None:
        parser.add_argument(
            "xcom_spec",
            metavar="DAG_ID/RUN_ID[/TASK_ID[/KEY]]",
            help=(
                "Retrieve the value of the 'return_value' XCom for the specified DAG run."
                " The format should be DAG_ID/DAG_RUN_ID/TASK_ID. This is useful for retrieving"
                " results from a DAG run after it has completed. If the specified DAG run or task"
                " does not exist. Only DAG_ID/DAG_RUN_ID is required, if TASK_ID and KEY are"
                " omitted, the client will attempt to retrieve all xcom entries for all tasks."
                " If only KEY is omitted, it will attempt to retrieve all xcom entries for the"
                " specified task."
            ),
        )

    def execute(self, **kwargs) -> None:
        logger.debug("Executing XComCommand with arguments", xcom_spec=self.app.args.xcom_spec)
        # xcom.get_xcom(self.app, self.app.args.xcom_spec)
        spec = ResourceSpec.from_string(self.app, self.app.args.xcom_spec)

        logger.debug(f"Getting XCom entry with specification {self.app.args.xcom_spec}", spec=spec)

        if spec.task_instance:  # Covers dag_id:dag_run_id:task_id and dag_id:dag_run_id:task_id:key
            logger.debug(
                "Retrieving XCom entries for specific task",
                dag=spec.dag,
                dag_run=spec.dag_run,
                task=spec.task_instance,
                key=spec.key,
            )
            spec.task_instance.get_xcom_entries(key=spec.key).output()
        else:  # dag_id:dag_run_id
            logger.debug(
                "Retrieving XCom entries for entire DAG run",
                dag=spec.dag,
                dag_run=spec.dag_run,
                key=spec.key,
            )
            spec.dag_run.xcoms.output()
