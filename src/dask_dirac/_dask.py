"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

import getpass
import hashlib
import logging
from typing import Any

from dask_jobqueue.core import Job, JobQueueCluster, cluster_parameters, job_parameters
from distributed.deploy.spec import ProcessInterface
from requests import get

logger = logging.getLogger(__name__)


def _get_site_ports(site: str) -> str:
    if site == "LCG.UKI-SOUTHGRID-RALPP.uk":
        return " --worker-port 50000:52000"

    return " "  # None


class DiracJob(Job):
    """Job class for Dirac"""

    config_name = "htcondor"  # avoid writing new one for now

    def __init__(
        self,
        scheduler: Any = None,
        name: str | None = None,
        config_name: str | None = None,
        submission_url: str | None = None,
        user_proxy: str | None = None,
        cert_path: str | None = None,
        jdl_file: str | None = None,
        owner_group: str | None = None,
        dirac_site: str | None = None,
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
            jdl_file = (
                "/tmp/dask-dirac-JDL_"
                + hashlib.sha1(getpass.getuser().encode("utf-8")).hexdigest()[:8]
            )
        if cert_path is None:
            cert_path = "/etc/grid-security/certificates"
        if owner_group is None:
            owner_group = "dteam_user"

        # public_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
        public_address = get("https://v4.ident.me/", timeout=30).content.decode("utf8")
        singularity_args = f"exec --cleanenv docker://sameriksen/dask:python3.10.9 dask worker tcp://{public_address}:8786"
        jdl_template = """
JobName = "dask-dirac: dask worker";
Executable = "singularity";
Arguments = \"{executable_args}\";
StdOutput = "std.out";
StdError = "std.err";
OutputSandbox = {{"std.out","std.err"}};
OwnerGroup = {owner};
""".lstrip()
        if dirac_site is not None:
            singularity_args += _get_site_ports(dirac_site)
            jdl_template += f"""
Site = {dirac_site!r};
""".lstrip().replace(
                "'", '"'
            )

        # Write JDL
        with open(jdl_file, mode="w", encoding="utf-8") as jdl:

            jdl.write(
                jdl_template.format(executable_args=singularity_args, owner=owner_group)
            )

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
