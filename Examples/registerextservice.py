"""
Module containing a decorator for Extension Services.

For the test framework a mapping is needed between 
Extension Services and jobs. So all dependent jobs are tested
when an Extension Service changes
"""
import contextlib
import json
from functools import wraps
from pathlib import Path

from gdpslib import utils

extServiceCallDepth = 0
persistedRdds = []


def _register(extService, jobCall, psGeneral, psLayer):
    """
    Register the Extension Service with the job.

    Saves the name of the job that uses the Extension Service to
    ``/mta/ext_mapping.json``. This file is used by the test framework
    to decide which tests to run.

    Parameters
    ----------
    extService : str
        Name of the Extension Service.
    jobCall : str
        Name of the job.
    psGeneral : dict like
        Parameterset with general settings.
    psLayer : dict like
        Parameterset with layer specific settings.
    """

    registryPath = Path("/dbfs/mnt/mta/ext_mapping.json")
    with utils.leaseBlob("ext_mapping.json", psGeneral, psLayer):
        try:
            with registryPath.open("r") as regFile:
                content = json.load(regFile)
        except (json.JSONDecodeError, FileNotFoundError):
            content = {}

        if extService in content:
            if jobCall in content[extService]:
                return
            else:
                content[extService].append(jobCall)
        else:
            content[extService] = [jobCall]

        with registryPath.open("w") as regFile:
            json.dump(content, regFile)


def registerExtService(isComplex):
    """
    Create an Extension Services decorator with registry and checkpoint logic.

    The created decorator must be used by all Extension Services to ensure
    that the mapping between Extension Service and job for the test framework
    is correct. The register part is only executed during test runs.

    When `isComplex` is set to ``True``, the output DataFrame is checkpointed
    locally to break the DAG. This can dramatically speed up the processing.

    Parameters
    ----------
    isComplex : bool
        Wether the Extension Service uses complex logic, e.g. many joins.

    Returns
    -------
    callable
        Decorator function.
    """

    def decorator(fcn):
        @wraps(fcn)
        def wrapper(*args, **kwargs):
            spark = kwargs["spark"]
            jobCall = kwargs["jobCall"]
            psGeneral = kwargs["psGeneral"]
            psLayer = kwargs["psLayer"]
            isTest = kwargs["isTest"]

            if isTest:
                _register(fcn.__name__, jobCall, psGeneral, psLayer)

            def newInnerFunc(dfInput):
                global extServiceCallDepth, persistedRdds
                extServiceCallDepth += 1
                # Result of the inner function is retrieved by first calling
                # the outer function to get the inner function object,
                # then call that with the input dataframe.
                originalInnerFunc = fcn(*args, **kwargs)
                dfOriginalOut = originalInnerFunc(dfInput)
                if isComplex:
                    if isTest:
                        # During testing we are running notebooks in parallel.
                        # Each has its own python interpreter process,
                        # but they see persisted rdds from all processes.
                        # So we need to break concurrency here to determine which RDD
                        # belongs to the current Ext Service
                        cm = utils.leaseBlob("persistingRdds", psGeneral, psLayer)
                    else:
                        # During normal job runs, clusters are isolated.
                        cm = contextlib.nullcontext()
                    with cm:
                        rddsBefore = spark._jsc.getPersistentRDDs().keys()
                        dfNewOut = dfOriginalOut.localCheckpoint()
                        rddsAfter = spark._jsc.getPersistentRDDs().items()
                    newRdds = [rdd for id_, rdd in rddsAfter if id_ not in rddsBefore]
                    if extServiceCallDepth == 1:
                        for rdd in persistedRdds:
                            rdd.unpersist()
                            persistedRdds = []

                    persistedRdds.extend(newRdds)
                else:
                    dfNewOut = dfOriginalOut

                extServiceCallDepth -= 1
                return dfNewOut

            return newInnerFunc

        return wrapper

    return decorator


__all__ = ["registerExtService"]
