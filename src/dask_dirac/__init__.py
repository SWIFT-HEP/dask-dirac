"""
Copyright (c) 2022 Luke Kreczko. All rights reserved.

dask-dirac: DIRAC Executor for Dask
"""

from __future__ import annotations

from ._dask import DiracClient, DiracCluster
from ._version import get_versions

__version__ = get_versions()["version"]
__all__ = ("__version__", "DiracCluster", "DiracClient")
