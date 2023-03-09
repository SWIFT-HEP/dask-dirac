"""Module for HTTP DIRAC queries
This is kept separate from the Dask DiracCluster implementation
since we might want to move it to a standalone dirac client
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import requests


@dataclass
class DiracSettings:
    """Settings for DIRAC queries"""

    server_url: str  # TODO: add validator
    capath: str = "/etc/grid-security/certificates"
    user_proxy: str = ""
    query_url: str = ""


def _query(settings: DiracSettings, params: dict[str, str]) -> Any:
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
