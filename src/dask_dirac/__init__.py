"""
Copyright (c) 2022 Luke Kreczko. All rights reserved.

dask-dirac: DIRAC Executor for Dask
"""


from __future__ import annotations

from ._dask import DiracCluster

__version__ = "0.1.0"

__all__ = ("__version__", "DiracCluster")
