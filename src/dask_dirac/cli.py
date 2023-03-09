"""Command line interface for dask-dirac"""
from __future__ import annotations

from typing import Any

import typer

from . import __version__, _dirac

app = typer.Typer()


@app.command()
def whoami(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Print the DN of the current user as seen by DIRAC server"""
    # settings could also be done via callback
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.whoami(settings)
    typer.echo(result)


@app.command()
def submit(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    jdl_file: str,  # "job.jdl"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
    # pylint: disable=unused-argument
    dask_script: str = typer.Option(
        default="who_cares_what_is_here", help="IGNORE THIS. workaround to ignore dask"
    ),
) -> None:
    """Submit a job to DIRAC server"""
    with open(jdl_file, encoding="utf-8") as file_handle:
        jdl = file_handle.read()
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.submit_job(settings, jdl)
    typer.echo(result)


@app.command()
def status(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Print the list of jobs"""
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.get_jobs(settings)
    typer.echo(result)


@app.command()
def get_max_parametric_jobs(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Print the maximum number of parametric jobs"""
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.get_max_parametric_jobs(settings)
    typer.echo(result)


@app.command()
def version() -> None:
    """Print the version number"""
    typer.echo(__version__)


def main() -> Any:
    """Entry point for the "xrdsum" command"""
    return app()
