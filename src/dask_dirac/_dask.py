"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

from typing import Any
from os import getcwd
import logging

from dask_jobqueue.core import Job, JobQueueCluster, cluster_parameters, job_parameters
from distributed.deploy.spec import ProcessInterface
from requests import get

logger = logging.getLogger(__name__)


class DiracJob(Job):
    """Job class for Dirac"""

    config_name = "htcondor"  # avoid writing new one for now
    scheduler_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
    singularity_args = f"exec --cleanenv docker://sameriksen/dask:debian dask-worker tcp://{scheduler_address}:8786"

    def __init__(
        self,
        scheduler: Any = None,
        name: str | None = None,
        config_name: str | None = None,
        submission_url: str | None = None,
        user_proxy: str | None = None,
        cert_path: str | None = None,
        jdl_file: str | None = None,
        **base_class_kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )


        if submission_url is None:
            submission_url = "https://lbcertifdirac70.cern.ch:8443"
        if user_proxy is None:
            user_proxy = "/tmp/x509up_u1000"
        if jdl_file is None:
            jdl_file = getcwd() + "/grid_JDL"
        if cert_path is None:
            cert_path = "/etc/grid-security/certificates"

        # Write JDL
        with open(jdl_file, mode="w", encoding="utf-8") as jdl:
            jdl_template = f"""
JobName = "dask_worker";
Executable = "singularity";
Arguments = "{DiracJob.singularity_args!s}";
StdOutput = "std.out";
StdError = "std.err";
OutputSandbox = {{"std.out","std.err"}};
OwnerGroup = "dteam_user";
            """.lstrip()

            jdl.write(jdl_template)

        self.submit_command = (
            "dask-dirac submit "
            + f"{submission_url} "
            + f"{jdl_file} "
            + f"--capath {cert_path} "
            + f"--user-proxy {user_proxy} "
            + "--dask-script "
        )


class DiracCluster(JobQueueCluster):  # pylint: disable=missing-class-docstring
    __doc__ = f""" Launch Dask on a cluster via Dirac

    Parameters
    ----------
    server_url: str
        URL to the DIRAC instance
    {job_parameters}
    {cluster_parameters}

    Examples
    --------
    >>> from dask_dirac import DiracCluster
    >>> cluster = DiracCluster(server_url="https://.....:8443", user_proxy="/tmp/X509_proxy")
    >>> cluster.scale(jobs=10)

    >>> from dask.distributed import Client
    >>> client = Client(cluster)
    """
    job_cls = DiracJob

    @classmethod
    def from_name(cls, name: str) -> ProcessInterface:
        """Create a cluster from a name"""
        return super().from_name(name)
