"""Module for HTTP DIRAC queries
This is kept separate from the Dask DiracCluster implementation
since we might want to move it to a standalone dirac client
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import gfal2
import requests


@dataclass
class DiracSettings:
    """Settings for DIRAC queries"""

    server_url: str  # TODO: add validator
    capath: str = "/etc/grid-security/certificates"
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


def add_file(
    settings: DiracSettings, local_file: str, remote_file: str, overwrite: bool
) -> Any:
    """Add file to directory on DIRAC server"""
    # example: https://github.com/cern-fts/gfal2-python/blob/develop/example/python/gfal2_copy.py
    # For now put everything under swift-hep at RAL site
    base_destination = (
        "https://mover.pp.rl.ac.uk:2880/pnfs/pp.rl.ac.uk/data/gridpp/swift-hep/"
    )
    destination = f"{base_destination}{local_file}"
    source = f"file://{local_file}"
    context = gfal2.creat_context()

    params = context.transfer_parameters()
    # don't set anything for now, expect an error
    # issue with gfal2 in dask-dirac env, needs investigation
    if overwrite:
        params.overwrite = True
    context.filecopy(params, source, destination)

    # lfns = 'LFN = "/gridpp/user/s/seriksen/test.npz";
    # Path = "/users/ak18773/SWIFT_HEP/dask-dirac/test.py";
    # SE="UKI-SOUTHGRID-RALPP-disk";\n'
    # lfns = "/users/ak18773/SWIFT_HEP/dask-dirac/test.py"

    # then do registration
    # endpoint = "DataManagement/FileCatalog"
    # settings.query_url = f"{settings.server_url}/{endpoint}"
    # params = {"method": "addFile", "args": json.dumps([lfns])}
    # return _query(settings, params)

    return None
