"""
Utility code for leases to manage concurrency.
"""
import os
import time
from contextlib import contextmanager
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils  # type: ignore


class TimeoutError(Exception):
    """
    Exception raised when the lease wait time is exceeded.

    When using a timeout value larger than 0, the job will only
    wait the given amount of seconds before raising this exception.
    """

    pass


@contextmanager
def leaseBlob(id, psGeneral, psLayer, timeout=-1):
    """
    Create an acquire a lock file on an ADLS account.

    Some G-DPS processes require serial execution.
    Because there is no connection between different spark clusters,
    locking needs to happen via a shared storage.
    The Databricks file system cannot be used for this, as its
    sync is slow and its behavior is not completely understood.

    Parameters
    ----------
    id : str
        Name of the lock file.
        Inside the lease context it is guaranteed,
        that only a single process with this id is
        running at any given time.
    psGeneral : dict like
        Parameterset with general settings.
        Holds the information for the Databricks
        service principal with access to the storage accounts.
    psLayer : dict like
        Parameterset with layer specific settings.
        Holds the information for the storage account
        and container where
        the lock file is created.
    timeout : int, optional
        Timeout in seconds, by default -1.
        When a lease cannot be acquired withing the given
        time, a :exc:`TimeoutError` is raised.
        For values <=0 the job is waiting indefinetly.

    Yields
    ------
    azure.storage.blob.BlobLeaseClient
        The acquired lease client object.

    Examples
    --------
    >>> registryPath = Path("/dbfs/mnt/mta/ext_mapping.json")
    >>> with utils.leaseBlob("ext_mapping.json", psGeneral, psLayer):
    ...     with registryPath.open("r") as regFile:
    ...         content = json.load(regFile)
    ...     content["new"] = "some value"
    ...     with registryPath.open("w") as regFile:
    ...         json.dump(content, regFile)
    """
    from azure.storage import blob
    from azure.core.exceptions import ResourceExistsError, HttpResponseError
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import StorageErrorCode as err

    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)

    scopeName = psGeneral["SECRET_SCOPE"]
    idKey = psGeneral["AZURE_CLIENT_ID_KEY"]
    secretKey = psGeneral["AZURE_CLIENT_SECRET_KEY"]
    os.environ["AZURE_TENANT_ID"] = psGeneral["AZURE_TENANT_ID"]
    os.environ["AZURE_CLIENT_ID"] = dbutils.secrets.get(scope=scopeName, key=idKey)
    os.environ["AZURE_CLIENT_SECRET"] = dbutils.secrets.get(scope=scopeName, key=secretKey)

    credential = DefaultAzureCredential()
    blobServiceClient = blob.BlobServiceClient(psLayer["AZURE_BLOB_URL"], credential=credential)
    blobContainerClient = blobServiceClient.get_container_client(psLayer["AZURE_CONTAINER_NAME"])
    blobClient = blobContainerClient.get_blob_client(f"leases/{id}")

    startTime = time.time()
    while True:
        try:
            blobClient.create_append_blob()
            lease = blobClient.acquire_lease()
            break
        except (HttpResponseError, ResourceExistsError) as e:
            if not e.error_code in [err.lease_id_missing, err.lease_already_present]:
                raise
            if timeout > 0:
                if (time.time() - startTime) >= timeout:
                    raise TimeoutError("Timeout while trying to acquire lease.") from e
            time.sleep(1)
    try:
        yield lease
    finally:
        blobClient.delete_blob(lease=lease)


__all__ = ["leaseBlob", "TimeoutError"]
