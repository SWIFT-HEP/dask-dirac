"""Module for HTTP DIRAC queries
This is kept separate from the Dask DiracCluster implementation
since we might want to move it to a standalone dirac client
"""

from __future__ import annotations

import json
import os
import zlib
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any

import _io

try:
    import gfal2  # pylint: disable=import-error
except ImportError:
    gfal2 = None
import requests


@dataclass
class DiracSettings:
    """Settings for DIRAC queries"""

    server_url: str  # TODO: add validator
    capath: str = "/cvmfs/grid.cern.ch/etc/grid-security/certificates/"
    user_proxy: str = ""
    query_url: str = ""


def _set_defaults(settings: DiracSettings, params: dict[str, str]) -> dict[str, str]:
    if "diracdev.grid.hep.ph.ic.ac.uk" in settings.query_url:
        params["clientSetup"] = params.get("clientSetup", "GridPP")
    return params


def _query(settings: DiracSettings, params: dict[str, str]) -> Any:
    params = _set_defaults(settings, params)

    result = requests.post(
        settings.query_url,
        data=params,
        cert=settings.user_proxy,
        verify=settings.capath,
        timeout=60,
    )
    return result.json()


def submit_job(settings: DiracSettings, jdl: str) -> Any:
    """Submit a job to a DIRAC server"""
    endpoint = "WorkloadManagement/JobManager"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "submitJob", "args": json.dumps([jdl])}
    return _query(settings, params)


def get_jobs(settings: DiracSettings) -> Any:
    """Get jobs from DIRAC server"""
    endpoint = "WorkloadManagement/JobMonitoring"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "getJobs"}
    return _query(settings, params)


def get_max_parametric_jobs(settings: DiracSettings) -> Any:
    """Get max parametric jobs from DIRAC server (mostly for testing)"""
    endpoint = "WorkloadManagement/JobManager"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "getMaxParametricJobs"}
    return _query(settings, params)


def whoami(settings: DiracSettings) -> Any:
    """Get user info from DIRAC server (mostly for testing)"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "whoami"}
    return _query(settings, params)


def get_directory_success_files(result: Any) -> list[str]:
    """Get files that are found in a directory"""

    sucessful_keys = result["Value"]["Successful"].keys()
    if len(sucessful_keys) == 0:
        return []
    all_successful_files = []
    for key in sucessful_keys:
        file_keys = list(result["Value"]["Successful"][key]["Files"].keys())
        all_successful_files.extend(file_keys)
    return all_successful_files


def get_directory_dump(settings: DiracSettings, lfns: str) -> Any:
    """Get directory dump from DIRAC server"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "getDirectoryDump", "args": json.dumps([lfns])}
    return _query(settings, params)


def create_directory(settings: DiracSettings, lfns: str) -> Any:
    """Create directory on DIRAC server"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "createDirectory", "args": json.dumps([lfns])}
    return _query(settings, params)


def remove_directory(settings: DiracSettings, lfns: str) -> Any:
    """Remove directory on DIRAC server"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "removeDirectory", "args": json.dumps([lfns])}
    return _query(settings, params)


def remove_file(settings: DiracSettings, lfns: str) -> Any:
    """Remove file to directory on DIRAC server"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "removeFile", "args": json.dumps([lfns])}
    return _query(settings, params)


def get_file(settings: DiracSettings, lfns: str) -> Any:
    """Download a file from DIRAC server"""
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"
    params = {"method": "getFile", "args": json.dumps([lfns])}
    return _query(settings, params)


def _adler32(file_path: str) -> str:
    """Calculate adler32 checksum of the supplied file"""

    def _read_chunk(
        file_descriptor: _io.BufferedReader, size: int = 1048576
    ) -> Generator[bytes]:
        """Read a chunk of data from a file-like object"""
        while True:
            data = file_descriptor.read(size)
            if not data:
                break
            yield data

    with open(file_path, "rb") as input_file:
        adler_decimal = 1
        for data in _read_chunk(input_file):
            adler_decimal = zlib.adler32(data, adler_decimal)

    # change checksum from decimal to hex
    adler_hex = (
        hex(adler_decimal & 0xFFFFFFFF)
        .lower()
        .replace("l", "")
        .replace("x", "0000")[-8:]
    )

    return adler_hex


def add_file(
    settings: DiracSettings, local_file: str, remote_file: str, overwrite: bool
) -> Any:
    """Add file to directory on DIRAC server"""
    # example: https://github.com/cern-fts/gfal2-python/blob/develop/example/python/gfal2_copy.py
    # For now put everything under swift-hep at RAL site
    base_destination = "https://mover.pp.rl.ac.uk:2880/pnfs/pp.rl.ac.uk/data"

    # upload the file to server
    destination = f"{base_destination}{remote_file}"
    source = f"file://{local_file}"
    context = gfal2.creat_context()

    params = context.transfer_parameters()

    if overwrite:
        params.overwrite = True

    context.filecopy(params, source, destination)

    # register file
    # if file is already registered, then this won't work.
    # Need to remove the file first.
    # TODO: remove file if --overwrite is being used
    endpoint = "DataManagement/FileCatalog"
    settings.query_url = f"{settings.server_url}/{endpoint}"

    lfns = {
        remote_file: {
            "PFN": remote_file,
            "SE": "UKI-SOUTHGRID-RALPP-disk",
            "Size": os.path.getsize(local_file),
            "Checksum": _adler32(local_file),
        }
    }

    params = {"method": "addFile", "args": json.dumps([lfns])}

    return _query(settings, params)
