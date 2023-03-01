"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

from typing import Any
from requests import get

from dask_jobqueue.core import Job, JobQueueCluster, cluster_parameters, job_parameters
from distributed.deploy.spec import ProcessInterface


class DiracJob(Job):
    """Job class for Dirac"""


    #
    # TODO: Move grid_url, proxy and certs into args

    config_name = "htcondor" # avoid writing new one for now
    scheduler_options={'port': '8786'} # unclear why but have to set this
    grid_url = "https://lbcertifdirac70.cern.ch:8443"
    user_proxy = "/tmp/x509up_u1000"
    certs = "/home/opc/diracos/etc/grid-security/certificates"
    scheduler_address = get('https://ifconfig.me').content.decode('utf8')
    singularity_args = "exec --cleanenv docker://sameriksen/dask:debian dask-worker tcp://{}:8786".format(scheduler_address)
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
        with open(DiracJob.jdl_file, 'w') as jdl:
            jdl.write('JobName = "dask_worker";\n')
            jdl.write('Executable = "singularity";\n')
            jdl.write('Arguments = "{0}";\n'.format(DiracJob.singularity_args))
            jdl.write('StdOutput = "std.out";\n')
            jdl.write('StdError = "std.err";\n')
            jdl.write('OutputSandbox = {"std.out","std.err"};\n')
            jdl.write('OwnerGroup = "dteam_user";')

        self.submit_command = ("dask-dirac submit "
                               + "%s " % DiracJob.grid_url
                               + "%s " % DiracJob.jdl_file
                               + "--capath %s " % DiracJob.certs
                               + "--user_proxy %s " % DiracJob.user_proxy
                               + "--dask_script ")


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
