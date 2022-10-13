# Databricks notebook source
"""
IDP load job for the I17_CONTRACT_NL information object.

We use the load job ``JB_IDP_IFRS_I17_CONTRACT_NL``
to calculate the desired columns by extension services
and to load these columns in the IDP target table.

Used ExtensionServices are:

* :mod:`~gdpslib.ifrs.idp.extensionservices.SHJobStartIDP`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHExtensionServiceValidationContract`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHExtensionServiceCohort`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHExtensionServicePortfolioIDIFG2`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHExtensionServiceUniqueIDFT`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHExtensionsServiceContractBusinessRecordDate`
* :mod:`~gdpslib.ifrs.idp.extensionservices.SHJobEndIDP`

Returns a json serialized dictionary with counts for total,
inserts, updates and deletes. Note that copies are not included
as they can be more calculated more easily from the other values.
For each key, the value is a list of lists holding the
``RCN_ID_RECONCILIATION`` and the corresponding counts.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## CONTRACT_NL IDP job
# MAGIC This job reads contract data from GSA and writes into IDP.
# MAGIC It uses extension services which are packed into a custom library (ExtensionServices) that is installed on the cluster.
# MAGIC This shows reusability and that the concepts used in the DataStage implementation can also be used in the new GDPS world.
# MAGIC
# MAGIC Here meta data is defined, most of this will be done by the meta data framework in the future and simply passed to the job.
# MAGIC For the sake of simplicity some of the values are hardcoded here and a subset is used to show that parametrization of jobs is working.
# MAGIC
# MAGIC Just as the original DataStage job, the job itself is merely a chain of extension services:
# MAGIC <img src='/files/images/contract_nl_ext.jpg'>

# COMMAND ----------

import gdpslib.ifrs.idp.extensionservices as ext
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from gdpslib import utils
import json

if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    dbutils.widgets.text("loadMode", "")
    dbutils.widgets.text("sourceSchema", "")
    dbutils.widgets.text("sourceTable", "")
    dbutils.widgets.text("targetSchema", "")
    dbutils.widgets.text("targetTable", "")
    dbutils.widgets.text("errorSchema", "")
    dbutils.widgets.text("errorTable", "")
    dbutils.widgets.text("psMeta", "")
    dbutils.widgets.text("jobRunId", "")
    dbutils.widgets.text("jobVisa", "")
    dbutils.widgets.text("jobStartTime", "")
    dbutils.widgets.text("keyColumns", "")
    dbutils.widgets.text("jobId", "")
    dbutils.widgets.text("jobName", "")
    dbutils.widgets.text("jobCall", "")
    dbutils.widgets.text("jobRunIdPrevious", "")
    dbutils.widgets.text("loadLimitField", "")
    dbutils.widgets.text("keySchema", "")
    dbutils.widgets.text("keyTable", "")
    dbutils.widgets.text("deltaLimits", "")
    dbutils.widgets.text("psLayer", "")
    dbutils.widgets.text("psGeneral", "")
    dbutils.widgets.text("isTest", "")

# COMMAND ----------


def execute(spark):
    """
    Run the job.

    This job performs a GSA to IDP load for the `I17_CONTRACT_NL`
    information object.

    Parameters
    ----------
    spark : pyspark.sql.session.SparkSession
        Instance of the spark session.

    Returns
    -------
    dict
        Dictionary with counts for total, inserts, updates and deletes.
        Note that copies are not included as they can be more calculated
        more easily from the other values.
        For each key, the value is a list of lists holding the
        ``RCN_ID_RECONCILIATION`` and the corresponding counts.
    """
    params = utils.paramDictFromBindings(dbutils)
    params["spark"] = spark

    # job execution
    dfData = ext.shJobStartIDP(**params)

    dfTransformed = (
        dfData.transform(ext.shExtensionServiceValidationContract(**params))
        .transform(
            ext.shExtensionServiceCohort(
                COHORT="COHORT", START_DATE_OF_COVERAGE="START_DATE_OF_COVERAGE", **params
            )
        )
        .transform(ext.shExtensionServicePortfolioIDIFG2(**params))
        .transform(
            ext.shExtensionServiceUniqueIDFT(OPERATIONAL_SEGMENT="OPERATIONAL_SEGMENT", **params)
        )
        .transform(ext.shExtensionsServiceContractBusinessRecordDate(**params))
    )

    counts = ext.shJobEndIDP(dfTransformed, **params)
    return counts


# COMMAND ----------

if __name__ == "__main__":
    result = execute(spark)
    dbutils.notebook.exit(json.dumps(result))
