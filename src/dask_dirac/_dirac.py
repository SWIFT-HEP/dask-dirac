from __future__ import annotations

from typing import Any

import requests


def _query(url: str, params: dict[str, str], capath:str, user_proxy:str) -> Any:
    if not capath:
        capath = "/etc/grid-security/certificates"
    if not user_proxy:
        user_proxy = "/tmp/x509up_u1000"  # TODO: default path should be /tmp/x509up_u<user id>
    with requests.post(url, data=params, cert=user_proxy, verify=capath) as r:
        return r.json()


def submit_job(server: str, jdl: str, capath:str, user_proxy:str) -> Any:
    url = f"https://{server}:8443/WorkloadManagement/JobManager"
    params = {"method": "submitJob", "jobDesc": jdl}
    return _query(url, params, capath, user_proxy)


def get_jobs(server: str, capath:str, user_proxy:str) -> Any:
    url = f"https://{server}:8443/WorkloadManagement/JobMonitoring"
    params = {"method": "getJobs"}
    return _query(url, params, capath, user_proxy)


def whoami(server: str, capath:str, user_proxy:str) -> Any:
    url = f"https://{server}:8443/DataManagement/FileCatalog"
    params = {"method": "whoami"}
    return _query(url, params, capath, user_proxy)
