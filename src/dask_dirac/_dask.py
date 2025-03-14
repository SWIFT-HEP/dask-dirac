"""Definitions for DaskCluster"""

# from dask.distributed import
from __future__ import annotations

import getpass
import glob
import hashlib
import logging
import os
from collections.abc import Callable
from typing import Any

import dask.core
import pandas as pd
from dask.distributed import Client
from dask.highlevelgraph import HighLevelGraph
from dask_jobqueue.core import Job, JobQueueCluster, cluster_parameters, job_parameters
from distributed.deploy.spec import ProcessInterface
from requests import get

from . import _dirac
from .templates import get_template

logger = logging.getLogger(__name__)


def _get_site_ports(sites: list[str] | str) -> str:
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
        dirac_sites: list[str] | str | None = None,
        require_gpu: bool = False,
        container: str = "docker://sameriksen/dask:centos9",
        nthreads: int | None = None,
        **base_class_kwargs: dict[str, Any],
    ) -> None:
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )
        # public_address = get("https://ifconfig.me", timeout=30).content.decode("utf8")
        public_address = get("https://v4.ident.me/", timeout=30).content.decode("utf8")
        jdl_template = get_template("jdl.j2")

        extra_args = _get_site_ports(dirac_sites) if dirac_sites else ""
        extra_args += f" --nthreads {nthreads}" if nthreads else ""

        if isinstance(dirac_sites, str):
            dirac_sites = [dirac_sites]

        rendered_jdl = jdl_template.render(
            container=container,
            public_address=public_address,
            owner=owner_group,
            dirac_sites=dirac_sites,
            require_gpu=require_gpu,
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

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        """Set defaults to be able to use htcondor config. Not actually used with Dirac"""
        kwargs.setdefault("cores", 1)
        kwargs.setdefault("memory", "0.5GB")
        super().__init__(*args, **kwargs)

    @classmethod
    def from_name(cls, name: str) -> ProcessInterface:
        """Create a cluster from a name"""
        return super().from_name(name)


class DiracClient(Client):
    """Client for caching dask computations"""

    def __init__(self, *args, cache_location: str = "file:///tmp/dask-dirac-cache", **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(*args, **kwargs)
        self.cache_location = cache_location

    def _graph_to_futures(
        self,
        dsk: dict[str, Any] | HighLevelGraph,
        *args: dict[str, Any],
        **kwargs: dict[str, Any],
    ) -> Any:
        if not isinstance(dsk, HighLevelGraph):
            dsk = HighLevelGraph.from_collections(id(dsk), dsk, dependencies={})

        info = dsk.to_dict()
        logging.debug(
            "Input dask graph:\n%s\n---------\nperforming caching checks\n---------",
            info,
        )

        sorted_keys = dask.core.toposort(info)
        tmp_info: dict[str, Any] = {}
        for key in sorted_keys:
            value = info[key]
            hash_tuple = None
            hash_base = None
            value_for_hash = value
            check_layer = True
            logging.debug("Key: %s, Value: %s", key, value)
            logging.debug("Checking if tmp_keys: %s, are in %s", tmp_info.keys(), value)
            for t_key, t_value in tmp_info.items():
                try:
                    if t_key in value:
                        logging.debug("Found %s in %s", t_key, value)
                        index = value.index(t_key)

                        hash_base = t_value["hash"]  # [index]
                        temp_list = list(value_for_hash)
                        temp_list[index] = hash_base
                        value_for_hash = tuple(temp_list)
                        logging.debug(hash_base)
                        logging.debug(value_for_hash)
                        logging.debug(index)
                    elif t_key == value:
                        logging.debug("Found %s in %s", t_key, value)
                        hash_base = t_value["hash"][0]
                        check_layer = False
                        hash_tuple = hash_base
                except KeyError:  # ignore problem for now
                    continue

            if hash_tuple is None:
                # Now we have the hash_base,
                _, hash_tuple = generate_hash_from_value(value_for_hash)

            tmp_info[key] = {
                "value": value,
                "hash": hash_tuple,
                "check_layer": check_layer,
            }

        logging.debug("---------\nHashes: %s\n---------", tmp_info)

        # Now check layers that need to be checked, adding caching
        tmp_2 = {}
        for key, value in tmp_info.items():
            logging.debug("Checking %s... ", key)
            # Now we check if hash exist at some location
            input_func_tuple = value["value"]
            input_hash_tuple = value["hash"]
            if not value["check_layer"]:
                logging.debug("Not processing layer")
                tmp_2[key] = input_func_tuple
            else:
                func_tuple = check_functions_and_hashes(
                    input_func_tuple, input_hash_tuple, self.cache_location
                )
                tmp_2[key] = func_tuple

            logging.debug("final_function_tuple:\n%s", func_tuple)

        logging.debug("---------\nFinalized graph: %s\n---------", tmp_2)

        dsk = HighLevelGraph.from_collections(id(tmp_2), tmp_2, dependencies={})

        logging.debug(
            "---------\nFinalized High Level Graph: %s\n---------", dsk.to_dict()
        )

        return super()._graph_to_futures(dsk, *args, **kwargs)


def check_functions_and_hashes(
    func_tuple: Any, hash_tuple: Any, cache_location: str
) -> Any:
    """Check if functions and hashes exist in cache"""
    logging.debug("Checking func_tuple: %s", func_tuple)
    logging.debug("Checking hash_tuple: %s", hash_tuple)

    cached_files = get_cached_files(cache_location)
    logging.debug("Cached files: %s", cached_files)

    if len(func_tuple) > 2:
        logging.debug(
            "Need to think about how to do this, but for now just check first hash"
        )
        if hash_tuple[0] in cached_files:
            return (load_from_parquet, hash_tuple[0], cache_location)
        return (save_to_parquet, hash_tuple[0], (func_tuple), cache_location)

    # Get to the deepest level and replace
    if isinstance(hash_tuple, tuple) and isinstance(func_tuple, tuple):
        current_hash, nested_hash = hash_tuple
        current_func, nested_func = func_tuple

        if current_hash in cached_files:
            return (load_from_parquet, current_hash, cache_location)
        # Recursively process the nested tuple
        modified_nested_func = check_functions_and_hashes(
            nested_func, nested_hash, cache_location
        )
        return (
            save_to_parquet,
            current_hash,
            (current_func, modified_nested_func),
            cache_location,
        )

    # Base case: No more nested tuples
    if hash_tuple in cached_files:
        return (load_from_parquet, hash_tuple, cache_location)
    return (save_to_parquet, hash_tuple, (func_tuple), cache_location)


def generate_hash_from_value(value: tuple[Callable[..., Any]]) -> tuple[str, Any]:
    """Generate hash from value"""
    if isinstance(value, tuple):
        this_tuple = None

        # Catch when there is no left and right as at end of chain
        if len(value) == 1:
            left = value[0]
            right = ""
        else:
            left = value[0]
            right = value[1:]
            if len(right) == 1:
                right = right[0]

        logging.debug("left: %s", left)
        logging.debug("right: %s", right)

        # Process left side
        if callable(left):
            try:
                left_name = left.__name__
            except BaseException:  # noqa: B036
                # Lets assume it's functools.partial for now
                left_name = type(left).__name__
        else:
            left_name = str(left)

        # Process right side
        if isinstance(right, tuple):
            logging.debug("rerunning function...\nleft: %s\nright: %s", left, right)
            right_hash, this_tuple = generate_hash_from_value(right)
        else:
            if callable(right):
                if hasattr(right, "__name__"):
                    right_name = right.__name__
                elif hasattr(right, "file"):
                    # if FileMeta for AGC will end up here
                    right_name = right.file
                else:
                    right_name = type(right).__name__
            else:
                right_name = str(right)
            right_hash = right_name

        # Combine the names/hashes for final hash
        combined = left_name + right_hash
        final_hash = hashlib.sha3_384(combined.encode()).hexdigest()
        # TODO: Integrate with DiracClient
        if this_tuple is not None:
            hash_tuple = (final_hash, this_tuple)
        else:
            hash_tuple = final_hash

        logging.debug(
            "hash inputs: %s\nhash inputs: %s + %s\nhash: %s\nhash tuple: %s",
            value,
            left_name,
            right_hash,
            final_hash,
            hash_tuple,
        )

        return final_hash, hash_tuple

    # should in theory never reach here
    return str(value), str(value)


def save_to_parquet(
    filename: str, data: pd.DataFrame, cache_location: str
) -> pd.DataFrame:
    """Save data to Parquet file.
    TODO: Make more generic than dataframe.
    """

    logging.debug("Writing stage to: %s/%s.parquet", cache_location, filename)

    # ensure dataframe columns have names
    if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
    if not all(isinstance(col, str) for col in data.columns):
        data.columns = [f"col_{i}" for i in range(data.shape[1])]

    if cache_location.startswith("file://"):
        cache_location = cache_location[len("file://") :]
        # make sure the directory exists
        os.makedirs(cache_location, exist_ok=True)
        name = cache_location + "/" + filename + ".parquet"
        data.to_parquet(name)
        return data

    # TODO: RUCIO
    # TODO: DIRAC
    raise NotImplementedError(f"Caching is not implemented yet for {cache_location}")


def load_from_parquet(filename: str, cache_location: str) -> pd.DataFrame:
    """Load data from Parquet file."""
    logging.debug("Loading cached file: %s/%s.parquet", cache_location, filename)

    if cache_location.startswith("file://"):
        cache_location = cache_location[len("file://") :]
        name = cache_location + "/" + filename + ".parquet"
        return pd.read_parquet(name)

    # TODO: RUCIO
    # TODO: DIRAC
    raise NotImplementedError(f"Caching is not implemented yet for {cache_location}")


def get_cached_files(cache_location: str) -> list[str]:
    """Get cached filed from cache location"""

    if cache_location.startswith("file://"):
        cache_location = cache_location[len("file://") :]
        file_list = glob.glob(cache_location + "/*.parquet")
        # remove parquet extension and get file name from path
        file_list = [c[c.rfind("/") + 1 : -8] for c in file_list]
        return file_list
    if cache_location.startswith("dirac://"):
        cache_location = cache_location[len("dirac://") :]
        # Hardcode for now
        server_url = "https://diracdev.grid.hep.ph.ic.ac.uk:8444"
        capath = "/cvmfs/grid.cern.ch/etc/grid-security/certificates/"
        user_proxy = "/tmp/x509up_u397871"
        settings = _dirac.DiracSettings(server_url, capath, user_proxy)
        result = _dirac.get_directory_dump(settings, cache_location)
        file_list = _dirac.get_directory_success_files(result)
        file_list = [file for file in file_list if file.endswith(".parquet")]
        file_list = [c[c.rfind("/") + 1 : -8] for c in file_list]
        return file_list

    # TODO: RUCIO
    # TODO: DIRAC
    raise NotImplementedError(f"Caching is not implemented yet for {cache_location}")
