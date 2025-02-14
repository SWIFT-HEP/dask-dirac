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
        default="who_cares_what_is_here",
        help="IGNORE THIS. workaround to ignore dask",
        hidden=True,
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
def get_directory_dump(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    lfns: str,  # "job.jdl"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Print to contents of a directory"""
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.get_directory_dump(settings, lfns)
    typer.echo(result)


@app.command()
def create_directory(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    lfns: str,  # "job.jdl"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Create a directory"""
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.create_directory(settings, lfns)
    typer.echo(result)


@app.command()
def remove_directory(
    server_url: str,  # "https://dirac.gridpp.ac.uk:8443"
    lfns: str,  # "job.jdl"
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Remove a directory"""
    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.remove_directory(settings, lfns)
    typer.echo(result)


@app.command()
def remove_file(
    server_url: str,
    lfns: str,
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Remove a file"""

    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.remove_file(settings, lfns)
    typer.echo(result)


@app.command()
def get_file(
    server_url: str,
    lfns: str,
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
) -> None:
    """Download a file to local directory"""

    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.get_file(settings, lfns)
    typer.echo(result)


@app.command()
def add_file(
    server_url: str,
    local_file: str,
    remote_file: str,
    capath: str = typer.Option(
        default="/etc/grid-security/certificates",
        help="path to CA certificate directory",
    ),
    user_proxy: str = typer.Option(
        default="/tmp/x509up_u1000", help="path to user proxy"
    ),
    overwrite: bool = typer.Option(default=False, help="overwrite existing file"),
) -> None:
    """Add a file"""

    settings = _dirac.DiracSettings(server_url, capath, user_proxy)
    result = _dirac.add_file(settings, local_file, remote_file, overwrite)
    typer.echo(result)


@app.command()
def version() -> None:
    """Print the version number"""
    typer.echo(__version__)


def main() -> Any:
    """Entry point for the "xrdsum" command"""
    return app()
