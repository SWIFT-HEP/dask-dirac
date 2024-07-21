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


def _get_site_ports(sites: list[str]) -> str:
    if "LCG.UKI-SOUTHGRID-RALPP.uk" in sites:
        return " --worker-port 50000:52000"

    return " "  # None


def _create_tmp_jdl_path() -> str:
    return (
        "/tmp/dask-dirac-JDL_"
        + hashlib.sha1(getpass.getuser().encode("utf-8")).hexdigest()[:8]
    )


def _get_graph_hash(graph: Any) -> str:

    total_graph_description = []
    for i, (layer_name, layer) in enumerate(graph.layers.items()):
        short_layer_name = layer_name[: layer_name.rfind("-")]
        _, task = next(iter(layer.items()))
        # function = task[0]
        task_args = task[1:]
        if i == 0:
            layer_args = task_args[0]
        else:
            layer_args = task_args[1]

        layer_description = {
            "function:": short_layer_name,
            "layer_length": len(layer.items()),
            "layer_args": layer_args,
        }
        total_graph_description.append(layer_description)

    return hashlib.sha3_384(str(total_graph_description).encode("utf-8")).hexdigest()


class DiracJob(Job):
    """Job class for Dirac"""

    config_name = "htcondor"  # avoid writing new one for now

    def __init__(
        self,
        scheduler: Any = None,
        name: str | None = None,
        config_name: str | None = None,
        submission_url: str = "https://diracdev.grid.hep.ph.ic.ac.uk:8444",
        user_proxy: str = "/tmp/x509up_u1000",
        cert_path: str = "/etc/grid-security/certificates",
        jdl_file: str = _create_tmp_jdl_path(),
        owner_group: str = "dteam_user",
        dirac_sites: list[str] | None = None,
        **base_class_kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )
        # public_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
        public_address = get("https://v4.ident.me/", timeout=30).content.decode("utf8")
        container = "docker://sameriksen/dask:centos9"
        jdl_template = get_template("jdl.j2")

        extra_args = _get_site_ports(dirac_sites) if dirac_sites else ""

        rendered_jdl = jdl_template.render(
            container=container,
            public_address=public_address,
            owner=owner_group,
            dirac_sites=dirac_sites,
            extra_args=extra_args,
        )

        # Write JDL
        with open(jdl_file, mode="w", encoding="utf-8") as jdl:
            jdl.write(rendered_jdl)

        cmd_template = get_template("submit_command.j2")
        self.submit_command = cmd_template.render(
            submission_url=submission_url,
            jdl_file=jdl_file,
            cert_path=cert_path,
            user_proxy=user_proxy,
        ).strip()


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
