import typer
from typing import Any

from . import _dirac
from . import __version__

app = typer.Typer()

@app.command()
def whoami(
    server_url: str, # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(default="/etc/grid-security/certificates", help="path to CA certificate directory"),
    user_proxy: str = typer.Option(default="/tmp/x509up_u1000", help="path to user proxy"),
    ) -> None:
    """Print the DN of the current user as seen by DIRAC server"""
    result = _dirac.whoami(server_url, capath, user_proxy)

@app.command()
def submit(
    server_url: str, # "https://dirac.gridpp.ac.uk:8443"
    jdl_file: str, # "job.jdl"
    capath: str = typer.Option(default="/etc/grid-security/certificates", help="path to CA certificate directory"),
    user_proxy: str = typer.Option(default="/tmp/x509up_u1000", help="path to user proxy"),
    ) -> None:
    """Submit a job to DIRAC server"""
    result = _dirac.submit_job(server_url, jdl, capath, user_proxy)

@app.command()
def status(
    server_url: str, # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(default="/etc/grid-security/certificates", help="path to CA certificate directory"),
    user_proxy: str = typer.Option(default="/tmp/x509up_u1000", help="path to user proxy"),
    ) -> None:
    """Print the list of jobs"""
    result = _dirac.get_jobs(server_url, capath, user_proxy)

@app.command()
def get_max_parametric_jobs(
    server_url: str, # "https://dirac.gridpp.ac.uk:8443"
    capath: str = typer.Option(default="/etc/grid-security/certificates", help="path to CA certificate directory"),
    user_proxy: str = typer.Option(default="/tmp/x509up_u1000", help="path to user proxy"),
    ) -> None:
    """Print the maximum number of parametric jobs"""
    result = _dirac.get_max_parametric_jobs(server_url, capath, user_proxy)

@app.command()
def version() -> None:
    """Print the version number"""
    typer.echo(__version__)


def main() -> Any:
    """Entry point for the "xrdsum" command"""
    return app()