from __future__ import annotations

from typing import Any

import requests


def _query(url: str, params: dict[str, str]) -> Any:
    capath = "/etc/grid-security/certificates"  # TODO: should be a parameter
    cert = "/tmp/x509up_u1000"  # TODO: default path should be /tmp/x509up_u<user id>
    with requests.post(url, data=params, cert=cert, verify=capath) as r:
        return r.json()


def submit_job(server: str, jdl: str) -> Any:
    url = f"https://{server}:8443/WorkloadManagement/JobManager"
    params = {"method": "submitJob", "jobDesc": jdl}
    return _query(url, params)


def get_jobs(server: str) -> Any:
    url = f"https://{server}:8443/WorkloadManagement/JobMonitoring"
    params = {"method": "getJobs"}
    return _query(url, params)


def whoami(server: str) -> Any:
    url = f"https://{server}:8443/DataManagement/FileCatalog"
    params = {"method": "whoami"}
    return _query(url, params)
