"""A module for interacting with Airflow DAGs, DAG runs, task instances, XCom and any other
Airflow entities via the Airflow API.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Union

from airflow_client.client.models import (
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
    DAGRunResponse,
    DagRunState,
    ResponseGetXcomEntry,
    TaskInstanceResponse,
    TaskInstancesLogResponse,
    TriggerDAGRunPostBody,
)

from airflow_api_client.io import PayloadConstructor, output
from airflow_api_client.log import logger

if TYPE_CHECKING:
    from airflow_api_client import App

TERMINAL_DAGRUN_STATES = {DagRunState.SUCCESS, DagRunState.FAILED}

# pylint: disable=protected-access

# pylint: disable=too-many-arguments,too-many-positional-arguments


class ResourceSpec:
    """A class representing the specification for retrieving XCom entries, including the DAG ID, run
    ID, optional task ID, and optional key. This class is used to parse and validate the XCom
    specification provided by the user.
    """

    def __init__(
        self,
        app: "App",
        dag: "DAG",
        dag_run: "DAGRun",
        task_instance: "TaskInstance",
        key: Optional[Union[str, None]] = None,
    ) -> None:
        self.app = app
        self.dag = dag
        self.dag_run = dag_run
        self.task_instance = task_instance
        self.key = key

    def __repr__(self) -> str:
        """Return a string representation of the ResourceSpec instance, including the DAG ID, Run
        ID and optionally task ID, and key.
        """
        task_id = self.task_instance.id if self.task_instance else None
        return (
            f"ResourceSpec(dag_id='{self.dag.id}', run_id='{self.dag_run.id}',"
            f" task_id='{task_id}', key='{self.key}')"
        )

    @classmethod
    def from_string(cls, app: "App", spec: str) -> "ResourceSpec":
        """Parse a resource specification string in the format DAG_ID/DAG_RUN_ID[/TASK_ID[/KEY]] and
        return a ResourceSpec instance with the corresponding attributes. If the specification
        string is not in the correct format, raise a ValueError with a descriptive error message.
        """

        parts = spec.split("/", 3)
        if len(parts) < 2:
            raise ValueError(
                "Invalid specification format. Expected DAG_ID/DAG_RUN_ID[/TASK_ID[/KEY]]"
            )

        dag_id, run_id = parts[0], parts[1]
        task_id = parts[2] if len(parts) > 2 else None
        key = parts[3] if len(parts) > 3 else None

        dag = DAG(app, dag_id)
        dag_run = DAGRun(app, dag, run_id)
        task_instance = TaskInstance(app, dag, dag_run, task_id) if task_id is not None else None

        return cls(app, dag, dag_run, task_instance, key)


class XComEntry:
    """A class representing an XCom entry in Airflow, providing lazy loading of the XCom data and
    delegation of attribute access to the underlying XCom data object.
    """

    def __init__(
        self,
        app: "App",
        dag: "DAG",
        dag_run: "DAGRun",
        task_instance: "TaskInstance",
        key: str,
        xcom_data=None,
    ) -> None:
        self.app = app
        self.dag = dag
        self.dag_run = dag_run
        self.task_instance = task_instance
        self.key = key
        self._xcom_data = xcom_data

    def __repr__(self) -> str:
        """Return a string representation of the XComEntry instance, including the key, DAG ID, task
        ID, and run ID.
        """
        return (
            f"xcom_entry(key='{self.key}', dag_id='{self.dag.id}',"
            f" task_id'{self.task_instance.task_instance.id}',"
            f" run_id='{self.dag_run.id}')"
        )

    @property
    def xcom_data(self) -> ResponseGetXcomEntry:
        """Lazily load the XCom data from the Airflow API if it hasn't been loaded yet, and return
        the underlying XCom data object.
        """
        if self._xcom_data is None:
            self._xcom_data = self.app.client._xCom_api.get_xcom_entry(
                dag_id=self.dag.id,
                task_id=self.task_instance.id,
                dag_run_id=self.dag_run.id,
                xcom_key=self.key,
            ).actual_instance
        return self._xcom_data

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying xcom data object"""
        return getattr(self.xcom_data, item)

    @classmethod
    def from_key(
        cls, app: "App", dag: "DAG", task_instance: "TaskInstance", dag_run: "DAGRun", key: str
    ) -> "XComEntry":
        """Fetch a single XCom entry for the given key from the Airflow API."""
        logger.debug(
            f"Fetching XCom entry for task '{task_instance.id}' in DAG run"
            f" '{dag_run.id}' with key '{key}'"
        )
        xcom_entry_data = app.client._xCom_api.get_xcom_entry(
            dag_id=dag.id, task_id=task_instance.id, dag_run_id=dag_run.id, xcom_key=key
        ).actual_instance
        return cls(
            app=app,
            dag=dag,
            task_instance=task_instance,
            dag_run=dag_run,
            key=key,
            xcom_data=xcom_entry_data,
        )


class XComEntryList:
    """A class representing a list of XCom entries for a given list of entries"""

    def __init__(
        self, app: "App", dag: "DAG", dag_run: "DAGRun", xcoms: Optional[List[XComEntry]] = None
    ) -> None:
        self.app = app
        self.dag = dag
        self.dag_run = dag_run
        self.xcoms = xcoms or []

    def add(self, entry: XComEntry) -> None:
        """Add an XComEntry to the list of entries"""
        self.xcoms.append(entry)

    def __iter__(self) -> Iterator[XComEntry]:
        """Return an iterator over the list of XCom entries"""
        return iter(self.xcoms)

    def to_dict(self) -> Dict[str, Any]:
        """Return the list of XCom entries as a dictionary, grouped by task ID and key"""
        _output = {"dag_id": self.dag.id, "dag_run_id": self.dag_run.id, "tasks": {}}
        for xcom in self.xcoms:
            _output["tasks"].setdefault(xcom.task_instance.id, {})[xcom.key] = {
                "value": xcom.value,
                "timestamp": xcom.timestamp,
            }
        return _output

    def output(self) -> None:
        """Output the list of XCom entries in the specified output format (JSON, YAML, or
        human-readable). The output is grouped by task ID and key, and includes the value and
        timestamp for each XCom entry. If the output format is JSON or YAML, the output is printed
        as a single structured document. If the output format is human-readable, the output is
        printed as a table with columns for task ID, key, timestamp, and value, sorted by timestamp.
        """

        if self.app.args.output_format in ("json", "yaml"):
            output(self.to_dict(), self.app.args)
        else:
            data = []
            headers = ["Task ID", "Key", "Timestamp", "Value"]
            for xcom in self:
                data.append(
                    [
                        xcom.task_instance.id,
                        xcom.key,
                        xcom.timestamp,
                        xcom.value,
                    ]
                )
            output(data, self.app.args, headers=headers)


class Log:
    """A class representing the log for a task instance in Airflow, providing a method to fetch the
    log content from the Airflow API.
    """

    def __init__(
        self,
        app: "App",
        dag: "DAG",
        dag_run: "DAGRun",
        task_instance: "TaskInstance",
        log_data: "TaskInstancesLogResponse" = None,
        try_number: int = 1,
    ) -> None:
        self.app = app
        self.dag = dag
        self.dag_run = dag_run
        self.task_instance = task_instance
        self._log_data = log_data
        self.try_number = try_number

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying logresponse object"""
        return getattr(self.log_data, item)

    @property
    def log_data(self) -> str:
        """Fetch the log content for this task instance from the Airflow API and return it as a
        string."""

        if self._log_data is not None:
            return self._log_data
        logger.debug(
            f"Fetching log for task instance '{self.task_instance.id}' in DAG run"
            f" '{self.dag_run.id}' of DAG '{self.dag.id}' from Airflow API"
        )
        self._log_data = self.app.client._task_instance_api.get_log(
            dag_id=self.dag.id,
            dag_run_id=self.dag_run.id,
            task_id=self.task_instance.id,
            try_number=self.try_number,
        )

        return self._log_data

    def __iter__(self) -> Iterator[Dict]:
        """Return an iterator over the list of log entries, returning a dictionary with the
        timestamp and log event for each log entry."""
        for log_line in self.log_data.to_dict()["content"]:
            if (
                log_line.timestamp is None
                and log_line.event.startswith("::")
                and "group::" in log_line.event
            ):
                # These are special log lines that indicate the start or end of a log stream, and
                # they don't have a timestamp. We can skip these lines since they don't serve
                # any purpose in a CLI
                continue
            yield {"timestamp": log_line.timestamp, "event": log_line.event}

    def output(self, time_format: str = "%Y-%m-%dT%H:%M:%S", table_format: str = "plain") -> None:
        """Output the log content in a human-readable format, with timestamps formatted according to
        the specified time format.
        table_format defaults to `plain` as the log contents is quite verbose and arbitrary. Too
        many borders look messy and is hard to read or copy-and-paste from.
        """

        rendered = []
        for log_event in self:
            event_time = (
                log_event["timestamp"].strftime(time_format) if log_event["timestamp"] else ""
            )
            rendered.append([event_time, log_event["event"]])

        output(
            rendered,
            self.app.args,
            headers=["Timestamp", "Log Event"],
            tablefmt=table_format,
        )


class TaskInstance:
    """A class representing a task instance in Airflow, providing methods to retrieve XCom entries
    for the task instance and to represent the task instance as a string.
    """

    def __init__(
        self,
        app: "App",
        dag: "DAG",
        dag_run: "DAGRun",
        task_id: str,
        task_instance: Optional["TaskInstance"] = None,
    ) -> None:
        self.app = app
        self.dag = dag
        self.id = task_id
        self.dag_run = dag_run
        self._task_instance = task_instance

    @property
    def task_instance(self) -> TaskInstanceResponse:
        """Lazily load the task instance details from the Airflow API if they haven't been loaded
        yet, and return the underlying task instance details object.
        """
        if self._task_instance is None:
            logger.debug(
                f"Fetching details for task instance '{self.id}' in DAG run '{self.dag_run.id}'"
                f" of DAG '{self.dag.id}' from Airflow API"
            )
            self._task_instance = self.app.client._task_instance_api.get_task_instance(
                dag_id=self.dag.id, dag_run_id=self.dag_run.id, task_id=self.id
            )
        return self._task_instance

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying DAG details object. If the DAG details
        haven't been loaded yet, we need to fetch them from the Airflow API before trying to access
        the attribute. If the attribute still isn't found after fetching the DAG details, we raise
        an AttributeError to indicate that the attribute doesn't exist on the DAG object.
        """
        return getattr(self.task_instance, item)

    def get_xcom_entries(self, key: Optional[Union[None, str]] = None) -> XComEntryList:
        """Fetch XCom entries for this task instance from the Airflow API. If a key is provided,
        fetch only the XCom entry for that key; otherwise, fetch all XCom entries
        for this task instance.
        """
        xcoms = XComEntryList(self.app, self.dag, self.dag_run)
        if key is not None:
            logger.debug(
                f"Fetching XCom entries for task '{self.id}' in DAG run"
                f" '{self.dag_run.id}' with key '{key}'"
            )

            xcoms.add(
                XComEntry.from_key(
                    app=self.app,
                    dag=self.dag,
                    dag_run=self.dag_run,
                    task_instance=self,
                    key=key,
                )
            )
            return xcoms

        # Getting all XCom entries for the task instance

        xcom_entries_data = self.app.client._xCom_api.get_xcom_entries(
            dag_id=self.dag.id, task_id=self.id, dag_run_id=self.dag_run.id
        ).xcom_entries

        for entry in xcom_entries_data:
            xcoms.add(
                XComEntry.from_key(
                    app=self.app,
                    dag=self.dag,
                    dag_run=self.dag_run,
                    task_instance=self,
                    key=entry.key,
                )
            )

        return xcoms

    def get_log(self, try_number: Union[None, int] = None) -> str:
        """Fetch the log for this task instance from the Airflow API and return it as a string."""
        try_number = try_number or self.try_number
        return Log(
            app=self.app,
            dag=self.dag,
            dag_run=self.dag_run,
            task_instance=self,
            try_number=try_number,
        )

    def __repr__(self) -> str:
        return f"TaskInstance(dag='{self.dag}', task_id='{self.id}'," f" dag_run='{self.dag_run}')"


# pylint: enable=too-many-arguments,too-many-positional-arguments


class DAGRun:
    """A class representing a DAG run in Airflow, providing methods to retrieve the state of the DAG
    run, fetch task instances for the DAG run, and represent the DAG run as
    a string.
    """

    def __init__(
        self, app: "App", dag: "DAG", run_id: str, dag_run: Optional[DAGRunResponse] = None
    ) -> None:
        self.app = app
        self.dag = dag
        self.id = run_id
        self._has_finished = False  # To keep track of whether we need to re-fetch the attributes
        self._dag_run = dag_run

        # If we're instantiated with an object that we know is already in a terminal state, we can
        # set the _has_finished flag to True right away to avoid unnecessary API calls later on when
        # checking the state or other attributes of the DAG run.
        if self._dag_run and self._dag_run.state in TERMINAL_DAGRUN_STATES:
            self._has_finished = True

    def __repr__(self) -> str:
        """Return a string representation of the DAGRun instance, including the DAG ID and run
        ID."""
        return f"DAGRun(dag='{self.dag}', run_id='{self.id}'"

    @property
    def dag_run(self) -> DAGRunResponse:
        """Lazily load the DAG run attributes from the Airflow API if they haven't been loaded yet,
        and return the attributes. If the DAG run has finished, we can return the cached attributes
        without re-fetching from the API, since the state won't change anymore.
        """
        if self._dag_run is not None and self._has_finished:
            return self._dag_run
        logger.debug(f"Fetching attributes for DAG run '{self.dag.id}' from Airflow API")
        self._dag_run = self.app.client._DAG_run_api.get_dag_run(
            dag_id=self.dag.id, dag_run_id=self.id
        )

        return self._dag_run

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying DAG run attributes object. If the DAG run
        attributes haven't been loaded yet, we need to fetch them from the Airflow API before
        trying to access the attribute. If the attribute still isn't found after fetching the DAG
        run attributes, we raise an AttributeError to indicate that the attribute doesn't exist on
        the DAG run object.
        """
        try:
            return getattr(self.dag_run, item)
        except AttributeError:
            if not isinstance(self._dag_run, DAGRunResponse):
                self._dag_run = self.app.client._DAG_run_api.get_dag_run(
                    dag_id=self.dag.id, dag_run_id=self.id
                )
                try:
                    return getattr(self._dag_run, item)
                except AttributeError:
                    pass
            raise AttributeError(f"'DAGRun' object has no attribute '{item}'") from None

    @property
    def state(self) -> str:
        """Return the state of the DAG run. If the DAG run has finished, we can return the cached
        state without re-fetching from the API, since the state won't change anymore. If the DAG run
        is still running, we need to fetch the latest state from the API to ensure we have the most
        up-to-date information.
        """
        if self._has_finished:
            return self.dag_run.state
        if (state := self.dag_run.state) in TERMINAL_DAGRUN_STATES:
            self._has_finished = True
        return state

    @property
    def task_instances(self) -> list[TaskInstance]:
        """Retrieve task instances for this DAG run from the Airflow API."""
        logger.debug(
            f"Fetching task instances for DAG run '{self.id}'"
            f" of DAG '{self.dag.id}' from Airflow API"
        )
        task_instances_data = sorted(
            self.app.client._task_instance_api.get_task_instances(
                dag_id=self.dag.id, dag_run_id=self.id
            ).task_instances,
            key=lambda ti: ti.end_date or ti.start_date or ti.execution_date,
        )

        return [
            TaskInstance(
                app=self.app,
                dag=self.dag,
                dag_run=self,
                task_id=instance.task_id,
                task_instance=instance,
            )
            for instance in task_instances_data
        ]

    @property
    def xcoms(self) -> XComEntryList:
        """Return all XCom entries for this DAG run by fetching the task
        instances for the DAG run and then fetching the XCom entries for each task instance. If
        `do_output` is set to `True`, the XCom entries are also printed to the console in the
        specified output format (JSON, YAML, or human-readable) using the `output` function from the
        `output` module.
        """
        xcoms = XComEntryList(self.app, self.dag, self)
        for ti in self.task_instances:
            for xcom in ti.get_xcom_entries():
                xcoms.add(xcom)
        return xcoms

    def get_logs(self, try_number: int = 1) -> Dict[str, Log]:
        """Return a dictionary of logs for each task instance in this DAG run, where the keys are
        task IDs and the values are Log objects representing the logs for each task instance. The
        logs are fetched lazily from the Airflow API when accessed, so they won't be fetched until
        you try to access the log for a specific task instance.
        """
        return {ti.id: list(ti.get_log(try_number=try_number)) for ti in self.task_instances}


class DAG:
    """A class representing a DAG in Airflow, providing methods to trigger a DAG run, retrieve DAG
    details, and delegate attribute access to the underlying DAG details object.
    """

    def __init__(
        self, app: "App", dag_id: str, dag: Optional[Union[None, DAGDetailsResponse]] = None
    ) -> None:
        self.app = app
        self.id = dag_id
        self._dag = dag

    def __repr__(self) -> str:
        """Return a string representation of the DAG instance, including the DAG ID."""
        return f"DAG(dag_id='{self.id}')"

    def run(
        self, run_id: Optional[Union[str, None]] = None, conf: Optional[Union[Dict, None]] = None
    ) -> DAGRun:
        """Trigger a DAG run for this DAG. If a run ID is provided, use that as the run ID for the
        DAG run; otherwise, generate a unique run ID based on the current timestamp. If a
        configuration dictionary is provided, include that in the payload for triggering
        the DAG run.
        """
        now_utc = datetime.now(timezone.utc)
        run_id = run_id or f"manual__{int(now_utc.timestamp())}"
        payload = {
            "conf": conf["conf"] if conf is not None else None,
            "dag_run_id": run_id,
            "logical_date": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        logger.debug(f"Triggering DAG '{self.id}' with payload", payload=payload)
        run_payload = TriggerDAGRunPostBody(**payload)
        result = self.app.client._DAG_run_api.trigger_dag_run(
            dag_id=self.id, trigger_dag_run_post_body=run_payload
        )
        logger.info("DAG run triggered successfully", dag_id=self.id, response=result)
        return DAGRun(self.app, dag=self, run_id=run_id)

    def get_dag_runs(self, limit) -> list[DAGRun]:
        """Fetch DAG runs for this DAG from the Airflow API, limited to the specified number of
        results."""
        dag_runs_data = self.app.client._DAG_run_api.get_dag_runs(
            dag_id=self.id, limit=limit, order_by=["-start_date"]
        ).dag_runs
        return [
            DAGRun(self.app, dag=self, run_id=dag_run.dag_run_id, dag_run=dag_run)
            for dag_run in dag_runs_data
        ]

    @property
    def dag(self) -> DAGDetailsResponse:
        """Lazily load the DAG details from the Airflow API if they haven't been loaded yet, and
        return the underlying DAG details object. If the DAG details have already been loaded,
        return the cached DAG details without re-fetching from the API, since the DAG details won't
        change after the DAG has been created.
        """
        if self._dag is not None:
            return self._dag
        self._dag = self.app.client._DAG_api.get_dag_details(dag_id=self.id)
        return self._dag

    def pause(self) -> DAGResponse:
        """Pause this DAG by sending a request to the Airflow API to update the DAG's `is_paused`
        attribute to `True`. This will prevent the DAG from being scheduled to run until it is
        resumed.
        """

        logger.debug(f"Pausing DAG '{self.id}' via Airflow API")
        return self.app.client._DAG_api.patch_dag(
            dag_id=self.id, dag_patch_body=DAGPatchBody(is_paused=True)
        )

    def unpause(self) -> DAGResponse:
        """Unpause this DAG by sending a request to the Airflow API to update the DAG's `is_paused`
        attribute to `False`.
        """
        logger.debug(f"Unpausing DAG '{self.id}' via Airflow API")
        return self.app.client._DAG_api.patch_dag(
            dag_id=self.id, dag_patch_body=DAGPatchBody(is_paused=False)
        )

    def create_payload(self) -> Dict[str, Any]:
        """Create a payload for triggering a DAG run by prompting the user for values for each
        parameter defined in the DAG's parameters.
        """
        constructor = PayloadConstructor(self.params)
        return constructor.construct_payload()

    def __getattr__(self, item) -> Any:
        """Delegate attribute access to the underlying DAG details object. If the DAG details
        haven't been loaded yet, we need to fetch them from the Airflow API before trying to access
        the attribute. If the attribute still isn't found after fetching the DAG details, we raise
        an AttributeError to indicate that the attribute doesn't exist on the DAG object.
        """
        try:
            return getattr(self.dag, item)
        except AttributeError:
            # If instantiated from DAGBag, the DAG details may not be available at this point,
            # so we need to fetch them before trying to access the attribute again.
            if not isinstance(self._dag, DAGDetailsResponse):
                self._dag = self.app.client._DAG_api.get_dag_details(dag_id=self.id)
                try:
                    return getattr(self._dag, item)
                except AttributeError:
                    pass
            raise AttributeError(f"'DAG' object has no attribute '{item}'") from None


class DAGBag:  # pylint: disable=too-few-public-methods
    """A class representing a collection of DAGs in Airflow, providing methods to retrieve all DAGs
    and delegate attribute access to the underlying Airflow API client for any attributes that
    aren't explicitly defined on the DAGBag class.
    """

    def __init__(self, app: "App") -> None:
        self.app = app

    def get_all_dags(self, details: Optional[bool] = True, **kwargs) -> list[DAG]:
        """Fetch all DAGs from the Airflow API. If the `details` parameter is set to `True`, fetch
        the full details for each DAG; otherwise, fetch only the basic information for each DAG. The
        `kwargs` parameter can be used to pass additional query parameters to the API request, such
        as `limit` or `offset`.
        """
        dags = self.app.client._DAG_api.get_dags(**kwargs).dags
        if not details:
            return [DAG(app=self.app, dag_id=dag.dag_id, dag=dag) for dag in dags]
        ret = []
        for dag in dags:
            ret.append(
                DAG(
                    app=self.app,
                    dag_id=dag.dag_id,
                    dag=self.app.client._DAG_api.get_dag_details(dag_id=dag.dag_id),
                )
            )
        return ret
