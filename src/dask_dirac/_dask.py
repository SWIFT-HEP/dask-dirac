"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

from typing import Any

from dask_jobqueue.core import Job, JobQueueCluster, cluster_parameters, job_parameters
from distributed.deploy.spec import ProcessInterface
from requests import get


class DiracJob(Job):
    """Job class for Dirac"""

    #
    # TODO: Move grid_url, proxy and certs into args

    config_name = "htcondor"  # avoid writing new one for now
    scheduler_options = {"port": "8786"}  # unclear why but have to set this
    grid_url = "https://lbcertifdirac70.cern.ch:8443"
    user_proxy = "/tmp/x509up_u1000"
    certs = "/home/opc/diracos/etc/grid-security/certificates"
    scheduler_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
    singularity_args = f"exec --cleanenv docker://sameriksen/dask:debian dask-worker tcp://{scheduler_address}:8786"
    jdl_file = "/home/opc/grid_JDL"

    def __init__(
        self,
        scheduler: Any = None,
        name: str | None = None,
        config_name: str | None = None,
        **base_class_kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        # Write JDL
        with open(DiracJob.jdl_file, mode="w", encoding="utf-8") as jdl:
            jdl_template = f"""
            JobName = "dask_worker";
            Executable = "singularity"
            Arguments = {DiracJob.singularity_args!r};
            StdOutput = "std.out";
            StdError = "std.err";
            OutputSandbox = {{"std.out","std.err"}};
            OwnerGroup = "dteam_user";
            """

            jdl.write(jdl_template)

        self.submit_command = (
            "dask-dirac submit "
            + f"{DiracJob.grid_url} "
            + f"{DiracJob.jdl_file} "
            + f"--capath {DiracJob.certs} "
            + f"--user_proxy {DiracJob.user_proxy} "
            + "--dask_script "
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
