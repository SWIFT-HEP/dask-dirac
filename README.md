# dask-dirac

[![Actions Status][actions-badge]][actions-link]
[![Documentation Status][rtd-badge]][rtd-link]
[![Code style: black][black-badge]][black-link]

[![PyPI version][pypi-version]][pypi-link]
[![PyPI platforms][pypi-platforms]][pypi-link]


[actions-badge]:            https://github.com/SWIFT-HEP/dask-dirac/workflows/CI/badge.svg
[actions-link]:             https://github.com/SWIFT-HEP/dask-dirac/actions
[black-badge]:              https://img.shields.io/badge/code%20style-black-000000.svg
[black-link]:               https://github.com/psf/black
[pypi-link]:                https://pypi.org/project/dask-dirac/
[pypi-platforms]:           https://img.shields.io/pypi/pyversions/dask-dirac
[pypi-version]:             https://badge.fury.io/py/dask-dirac.svg
[rtd-badge]:                https://readthedocs.org/projects/dask-dirac/badge/?version=latest
[rtd-link]:                 https://dask-dirac.readthedocs.io/en/latest/?badge=latest
[sk-badge]:                 https://scikit-hep.org/assets/images/Scikit--HEP-Project-blue.svg

dask-dirac is a library for launching a [dask](https://www.dask.org/) cluster with [DIRAC](https://dirac.readthedocs.io/en/latest/).

# Installation

dask-dirac can be installed [from PyPI](https://pypi.org/project/dask-dirac/) using pip.

```bash
pip install dask-dirac
```

# Requirements
1. Open port on localhost (default is 8786). Required for scheduler
2. DIRAC grid certificate

# Usage
A dask-dirac cluster can be setup in a similar fashion to other dask clusters.

```python
from dask_dirac import DiracCluster
from dask.distributed import Client

cluster = DiracCluster()
cluster.scale(jobs=5)
client = Client(cluster)
```

# Acknowledgements
<!-- readme: contributors -start -->
<table>
	<tbody>
		<tr>
            <td align="center">
                <a href="https://github.com/seriksen">
                    <img src="https://avatars.githubusercontent.com/u/5619270?v=4" width="100;" alt="seriksen"/>
                    <br />
                    <sub><b>Null</b></sub>
                </a>
            </td>
            <td align="center">
                <a href="https://github.com/kreczko">
                    <img src="https://avatars.githubusercontent.com/u/1213276?v=4" width="100;" alt="kreczko"/>
                    <br />
                    <sub><b>Luke Kreczko</b></sub>
                </a>
            </td>
		</tr>
	<tbody>
</table>
<!-- readme: contributors -end -->

## Bots
<!-- readme: bots -start -->
<table>
	<tbody>
		<tr>
            <td align="center">
                <a href="https://github.com/dependabot[bot]">
                    <img src="https://avatars.githubusercontent.com/in/29110?v=4" width="100;" alt="dependabot[bot]"/>
                    <br />
                    <sub><b>dependabot[bot]</b></sub>
                </a>
            </td>
            <td align="center">
                <a href="https://github.com/github-actions[bot]">
                    <img src="https://avatars.githubusercontent.com/in/15368?v=4" width="100;" alt="github-actions[bot]"/>
                    <br />
                    <sub><b>github-actions[bot]</b></sub>
                </a>
            </td>
		</tr>
	<tbody>
</table>
<!-- readme: bots -end -->
