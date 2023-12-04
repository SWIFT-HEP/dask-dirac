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

from .templates import get_template

logger = logging.getLogger(__name__)


def _get_site_ports(site: str) -> str:
    if site == "LCG.UKI-SOUTHGRID-RALPP.uk":
        return " --worker-port 50000:52000"

    return " "  # None


def _create_tmp_jdl_path() -> str:
    return (
        "/tmp/dask-dirac-JDL_"
        + hashlib.sha1(getpass.getuser().encode("utf-8")).hexdigest()[:8]
    )


class DiracJob(Job):
    """Job class for Dirac"""

    config_name = "htcondor"  # avoid writing new one for now

    def __init__(
        self,
        scheduler: Any = None,
        name: str | None = None,
        config_name: str | None = None,
        submission_url: str = "https://lbcertifdirac70.cern.ch:8443",
        user_proxy: str = "/tmp/x509up_u1000",
        cert_path: str = "/etc/grid-security/certificates",
        jdl_file: str = _create_tmp_jdl_path(),
        owner_group: str = "dteam_user",
        dirac_site: str | None = None,
        **base_class_kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )
        # public_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
        public_address = get("https://v4.ident.me/", timeout=30).content.decode("utf8")
        container = "docker://sameriksen/dask:centos9"
        jdl_template = get_template("jdl.j2")
        rendered_jdl = jdl_template.render(
            container=container,
            public_address=public_address,
            owner=owner_group,
            dirac_site=dirac_site,
        )

        # Write JDL
        with open(jdl_file, mode="w", encoding="utf-8") as jdl:
            jdl.write(rendered_jdl)

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
