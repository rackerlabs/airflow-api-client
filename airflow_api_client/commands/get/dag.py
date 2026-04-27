# Copyright 2026 Rackspace Technology, Inc. SPDX-License-Identifier: Apache-2.0
"""Get DAG details"""

from airflow_api_client.log import logger
from airflow_api_client.models import DAG
from airflow_api_client.typedefs import App


class GetDagCommand:
    """Helper class for getting DAG details"""

    @classmethod
    def add_subparser(cls, subparsers):
        """Add subparser for dag"""
        dag_parser = subparsers.add_parser("dag", help="Get DAG details")
        dag_parser.add_argument("dag_id", help="DAG ID")

    @staticmethod
    def get_dag(app: App) -> dict:
        """Get DAG details and return as dict"""
        dag = DAG(app, app.args.dag_id)
        logger.info(f"Retrieved details for DAG '{app.args.dag_id}'")
        return dag.to_dict()
