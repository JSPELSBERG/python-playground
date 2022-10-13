"""
Custom Databricks operators for GDPS.
"""
import airflow.providers.databricks.operators.databricks as dbop
from gdpsair.sensors.wasb import XCOM_NAMES

# TODO: this could use some refactoring to prevent duplicate code.
# All init methods are the same, except for the job_name, which is not
# used in UnknwonFileOperator. The basic idea would be to have an abstract base class
# defining an abstractproperty that determines if the job_name keyword parameter
# is needed, this then needs to be overwritten by each derived class.
# Problem is, if no __init__ is defined explicetely, the constructor
# signature is determined as *args, **kwargs, since we mock airflow in sphinx.
# If we do not mock airflow, we get import errors because it tries to
# import inherited attributes. This might be related to
# https://github.com/sphinx-doc/sphinx/issues/9884
# So if this bug is fixed we should look into refactoring this.


class GsaLoadOperator(dbop.DatabricksSubmitRunDeferrableOperator):
    """
    Deferrable version of ``DatabricksSubmitRunOperator`` for GSA loads.

    This is a special operator to start GSA loads.
    The file name is retrieved from XCOM and needs to be provided by
    a sensor, usually a :class:`~gpdsair.sensors.wasb:WasbRegexSensor`.

    Parameters
    ----------
    json : dict
        A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint.
        This field is templated.
    job_name : str
        GDPS IFRS job name to run, e.g. "ESGINS_I17_CONTRACT_NL".
    databricks_conn_id : str
        Reference to the `Databricks connection`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    polling_period_seconds : int
        Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    databricks_retry_limit : int
        Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    databricks_retry_delay : int or float
        Number of seconds to wait between retries (it
            might be a floating point number).
    databricks_retry_args : dict
        An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    do_xcom_push : bool
        Whether we should push run_id and run_page_url to xcom.
    **kwargs : dict
        Additional keyword arguments passed to ``DatabricksSubmitRunOperator``.

    Notes
    -----
    See `Databricks API doc <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`
    for more information.
    """

    def __init__(
        self,
        *,
        json=None,
        job_name=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=1,
        databricks_retry_delay=1,
        databricks_retry_args=None,
        do_xcom_push=True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if json is None:
            raise Exception("Error: need to provide a base json template")
        if job_name is None:
            raise Exception("Error: need to provide a i17 job name")
        self.json = json
        self.job_name = job_name
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args

        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        """
        Execute the GSA load operator.

        This is the main method that is executed by the Airflow scheduler.
        Refer to get_template_context for more context.

        Parameters
        ----------
        context : dict
            Same dictionary used as when rendering jinja templates.
            Contains information about the current task instance and dag run.
        """
        hook = self._get_hook()
        sensor_task_id = self.task_id[:2] + "T" + self.task_id[3:]
        file_name = context["ti"].xcom_pull(task_ids=sensor_task_id, key=XCOM_NAMES.SRC_FILE)
        prot_name = context["ti"].xcom_pull(task_ids=sensor_task_id, key=XCOM_NAMES.PROT_FILE)
        md5_name = context["ti"].xcom_pull(task_ids=sensor_task_id, key=XCOM_NAMES.MD5_FILE)
        notebook_path = "/Repos/live/gdps-ifrs-databricks/notebooks/gsa/SQ_GSA_LOAD"
        task = self.json["tasks"][0]
        task["libraries"] = [{"pypi": {"package": "fsspec"}}]
        task["notebook_task"]["notebook_path"] = notebook_path
        task["notebook_task"]["base_parameters"]["FILE_NAME"] = file_name
        if prot_name is not None:
            task["notebook_task"]["base_parameters"]["PROT_FILE_NAME"] = prot_name
        if md5_name is not None:
            task["notebook_task"]["base_parameters"]["MD5_FILE_NAME"] = md5_name
        task["notebook_task"]["base_parameters"]["JOB_NAME"] = self.job_name
        self.json["run_name"] = f"SQ_GSA_LOAD.{self.job_name}"
        self.json = dbop.deep_string_coerce(self.json)

        self.log.info(
            f"Executing notebook '{notebook_path}' with parameters:\n"
            f"{task['notebook_task']['base_parameters']}"
        )
        self.log.debug(f"Json payload in run submit REST call:\n {self.json}")
        self.run_id = hook.submit_run(self.json)

        dbop._handle_deferrable_databricks_operator_execution(self, hook, self.log, context)


class IdpLoadOperator(dbop.DatabricksSubmitRunDeferrableOperator):
    """
    Deferrable version of ``DatabricksSubmitRunOperator`` for IDP loads.

    This is a special operator to start IDP loads.

    Parameters
    ----------
    json : dict
        A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint.
        This field is templated.
    job_name : str
        GDPS IFRS job name to run, e.g. "ESGINS_I17_CONTRACT_NL".
    databricks_conn_id : str
        Reference to the `Databricks connection`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    polling_period_seconds : int
        Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    databricks_retry_limit : int
        Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    databricks_retry_delay : int or float
        Number of seconds to wait between retries (it
            might be a floating point number).
    databricks_retry_args : dict
        An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    do_xcom_push : bool
        Whether we should push run_id and run_page_url to xcom.
    **kwargs : dict
        Additional keyword arguments passed to ``DatabricksSubmitRunOperator``.

    Notes
    -----
    See `Databricks API doc <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`
    for more information.
    """

    def __init__(
        self,
        *,
        json=None,
        job_name=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=1,
        databricks_retry_delay=1,
        databricks_retry_args=None,
        do_xcom_push=True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if json is None:
            raise Exception("Error: need to provide a base json template")
        if job_name is None:
            raise Exception("Error: need to provide a i17 job name")
        self.json = json
        self.job_name = job_name
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args

        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        """
        Execute the IDP load operator.

        This is the main method that is executed by the Airflow scheduler.
        Refer to get_template_context for more context.

        Parameters
        ----------
        context : dict
            Same dictionary used as when rendering jinja templates.
            Contains information about the current task instance and dag run.
        """
        hook = self._get_hook()
        notebook_path = "/Repos/live/gdps-ifrs-databricks/notebooks/idp/SQ_IDP_LOAD"
        task = self.json["tasks"][0]
        task["notebook_task"]["notebook_path"] = notebook_path
        task["notebook_task"]["base_parameters"]["JOB_NAME"] = self.job_name
        self.json["run_name"] = f"SQ_IDP_LOAD.{self.job_name}"
        self.json = dbop.deep_string_coerce(self.json)

        self.log.info(
            f"Executing notebook '{notebook_path}' with parameters:\n"
            f"{task['notebook_task']['base_parameters']}"
        )
        self.log.debug(f"Json payload in run submit REST call:\n {self.json}")
        self.run_id = hook.submit_run(self.json)
        dbop._handle_deferrable_databricks_operator_execution(self, hook, self.log, context)


class UnknownFileOperator(dbop.DatabricksSubmitRunDeferrableOperator):
    """
    Deferrable version of ``DatabricksSubmitRunOperator`` for unknown files.

    This is a special operator to start the unknokn file processing.

    Parameters
    ----------
    json : dict
        A JSON object containing API parameters which will be passed
        directly to the ``api/2.1/jobs/runs/submit`` endpoint.
        This field is templated.
    databricks_conn_id : str
        Reference to the `Databricks connection`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    polling_period_seconds : int
        Controls the rate which we poll for the result of
        this run. By default the operator will poll every 30 seconds.
    databricks_retry_limit : int
        Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    databricks_retry_delay : int or float
        Number of seconds to wait between retries (it
            might be a floating point number).
    databricks_retry_args : dict
        An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
    do_xcom_push : bool
        Whether we should push run_id and run_page_url to xcom.
    **kwargs : dict
        Additional keyword arguments passed to ``DatabricksSubmitRunOperator``.

    Notes
    -----
    See `Databricks API doc <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit>`
    for more information.
    """

    def __init__(
        self,
        *,
        json=None,
        databricks_conn_id="databricks_default",
        polling_period_seconds=30,
        databricks_retry_limit=1,
        databricks_retry_delay=1,
        databricks_retry_args=None,
        do_xcom_push=True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if json is None:
            raise Exception("Error: need to provide a base json template")
        self.json = json
        self.databricks_conn_id = databricks_conn_id
        self.polling_period_seconds = polling_period_seconds
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.databricks_retry_args = databricks_retry_args

        # This variable will be used in case our task gets killed.
        self.run_id = None
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        """
        Execute the unknown file operator.

        This is the main method that is executed by the Airflow scheduler.
        Refer to get_template_context for more context.

        Parameters
        ----------
        context : dict
            Same dictionary used as when rendering jinja templates.
            Contains information about the current task instance and dag run.
        """
        hook = self._get_hook()

        file_names = context["ti"].xcom_pull(key=XCOM_NAMES.UNKNOWN_FILES)
        src_paths = context["ti"].xcom_pull(key=XCOM_NAMES.UNKNOWN_PATHS)

        notebook_path = "/Repos/live/gdps-ifrs-databricks/notebooks/gsa/SQ_UNKNOWN_FILE"
        task = self.json["tasks"][0]
        task["notebook_task"]["notebook_path"] = notebook_path
        task["notebook_task"]["base_parameters"]["FILE_NAMES"] = file_names
        task["notebook_task"]["base_parameters"]["SRC_PATHS"] = src_paths
        self.json["run_name"] = f"SQ_UNKNOWN_FILE"
        self.json = dbop.deep_string_coerce(self.json)

        self.log.info(
            f"Executing notebook '{notebook_path}' with parameters:\n"
            f"{task['notebook_task']['base_parameters']}"
        )
        self.log.debug(f"Json payload in run submit REST call:\n {self.json}")
        self.run_id = hook.submit_run(self.json)
        dbop._handle_deferrable_databricks_operator_execution(self, hook, self.log, context)
