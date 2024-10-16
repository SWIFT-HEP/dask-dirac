"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

import getpass
import glob
import hashlib
import logging
from typing import Any, Callable

import dask.core
import pandas as pd
from dask.distributed import Client
from dask.highlevelgraph import HighLevelGraph
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


class DiracClient(Client):
    """Client for caching dask computations"""

    def _graph_to_futures(
        self,
        dsk: dict[str, Any] | HighLevelGraph,
        *args: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> Any:
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies=dict())

        info = dsk.to_dict()
        logging.debug(
            f"Input dask graph:\n{info}\n---------\nperforming caching checks\n---------"
        )

        sorted_keys = dask.core.toposort(info)
        tmp_info: dict[str, Any] = {}
        for key in sorted_keys:
            value = info[key]
            hash_tuple = None
            hash_base = None
            value_for_hash = value
            check_layer = True
            logging.debug(f"Key: {key}, Value: {value}")
            logging.debug(f"Checking if tmp_keys: {tmp_info.keys()}, are in {value}")
            for t_key in tmp_info.keys():
                try:
                    if t_key in value:
                        logging.debug(f"Found {t_key} in {value}")
                        index = value.index(t_key)

                        hash_base = tmp_info[t_key]["hash"]  # [index]
                        temp_list = list(value_for_hash)
                        temp_list[index] = hash_base
                        value_for_hash = tuple(temp_list)
                        logging.debug(hash_base)
                        logging.debug(value_for_hash)
                        logging.debug(index)
                    elif t_key == value:
                        logging.debug(f"Found {t_key} in {value}")
                        hash_base = tmp_info[t_key]["hash"][0]
                        check_layer = False
                        hash_tuple = hash_base
                except BaseException:  # ignore problem for now
                    continue

            if hash_tuple is None:
                # Now we have the hash_base,
                _, hash_tuple = generate_hash_from_value(value_for_hash)

            tmp_info[key] = {
                "value": value,
                "hash": hash_tuple,
                "check_layer": check_layer,
            }

        logging.debug(f"---------\nHashes: {tmp_info}\n---------")

        # Now check layers that need to be checked, adding caching
        tmp_2 = {}
        for key in tmp_info.keys():
            logging.debug(f"Checking {key}... ")
            # Now we check if hash exist at some location
            input_func_tuple = tmp_info[key]["value"]
            input_hash_tuple = tmp_info[key]["hash"]
            if not tmp_info[key]["check_layer"]:
                logging.debug("Not processing layer")
                tmp_2[key] = input_func_tuple
            else:
                func_tuple = check_functions_and_hashes(
                    input_func_tuple, input_hash_tuple
                )
                tmp_2[key] = func_tuple

            logging.debug(f"final_function_tuple:\n{func_tuple}")

        logging.debug(f"---------\nFinalized graph: {tmp_2}\n---------")

        dsk = HighLevelGraph.from_collections(id(tmp_2), tmp_2, dependencies=dict())

        logging.debug(
            f"---------\nFinalized High Level Graph: {dsk.to_dict()}\n---------"
        )

        return super()._graph_to_futures(dsk, *args, **kwargs)


def check_functions_and_hashes(func_tuple: Any, hash_tuple: Any) -> Any:
    logging.debug(f"Checking func_tuple: {func_tuple}")
    logging.debug(f"Checking hash_tuple: {hash_tuple}")
    global cache_location
    cached_files = glob.glob(cache_location + "/*.parquet")
    cached_files = [c[c.rfind("/") + 1 : -4] for c in cached_files]

    if len(func_tuple) > 2:
        logging.debug(
            "Need to think about how to do this, but for now just check first hash"
        )
        if hash_tuple[0] in cached_files:
            return (load_from_parquet, hash_tuple[0])
        else:
            return (save_to_parquet, hash_tuple[0], (func_tuple))

    # Get to the deepest level and replace
    if isinstance(hash_tuple, tuple) and isinstance(func_tuple, tuple):
        current_hash, nested_hash = hash_tuple
        current_func, nested_func = func_tuple

        if current_hash in cached_files:
            return (load_from_parquet, current_hash)
        else:
            # Recursively process the nested tuple
            modified_nested_func = check_functions_and_hashes(nested_func, nested_hash)
            return (save_to_parquet, current_hash, (current_func, modified_nested_func))
    else:
        # Base case: No more nested tuples
        if hash_tuple in cached_files:
            return (load_from_parquet, hash_tuple)
        else:
            return (save_to_parquet, hash_tuple, (func_tuple))


def generate_hash_from_value(value: tuple[Callable[..., Any]]) -> tuple[str, Any]:
    if isinstance(value, tuple):

        # Catch when there is no left and right as at end of chain
        if len(value) == 1:
            left = value[0]
            right = ""
        else:
            left = value[0]
            right = value[1:]
            if len(right) == 1:
                right = right[0]

        logging.debug(f"left: {left}")
        logging.debug(f"right: {right}")

        # Process left side
        if callable(left):
            try:
                left_name = left.__name__
            except BaseException:  # Lets assume it's functools.partial for now
                left_name = type(left).__name__
        else:
            left_name = str(left)

        # Process right side
        if isinstance(right, tuple):
            logging.debug(f"rerunning function...\nleft: {left}\nright: {right}")
            right_hash, this_tuple = generate_hash_from_value(right)
        else:
            if callable(right):
                try:
                    right_name = right.__name__
                except BaseException:  # lets just assume it's FileMeta for now
                    right_name = right.file
            else:
                right_name = str(right)
            right_hash = right_name

        # Combine the names/hashes for final hash
        combined = left_name + right_hash
        final_hash = hashlib.sha3_384(combined.encode()).hexdigest()
        # TODO: Integrate with DiracClient
        if "this_tuple" in locals():
            hash_tuple = (final_hash, this_tuple)
        else:
            hash_tuple = final_hash

        logging.debug(
            f"hash inputs: {value}\nhash inputs: {left_name} + {right_hash}\nhash: {final_hash}\nhash tuple: {hash_tuple}"
        )

        return final_hash, hash_tuple

    # should in theory never reach here
    return str(value), str(value)


def save_to_parquet(filename: str, data: pd.DataFrame) -> pd.DataFrame:
    """Save data to Parquet file."""
    global cache_location
    name = cache_location + "/" + filename + ".parquet"

    logging.debug(f"Saving file to {name}")

    # TODO: Implement caching logic here

    return data


def load_from_parquet(filename: str) -> pd.DataFrame:
    """Load data from Parquet file."""
    global cache_location
    # TODO: Move cache location to DiracClient?
    name = cache_location + "/" + filename + ".parquet"
    return pd.read_parquet(name)
