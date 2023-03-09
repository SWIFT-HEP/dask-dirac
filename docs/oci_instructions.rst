OCI Instructions
================

Well... I didn't expect you to look here.
This page contains the notes that may oneday make it into actual documentation.


Oracle OCI
----------
OCI is free to have a 1 CPU, 1 GB instance (an AMD VM) - this is what I've been using.
It is also free to have a 4 GPU, 24GB instance on ARM.

Steps:
- Open port 8786 on subnet
- open port on node
- open port 8786 on subnet
- open port on node
- install dask-dirac
- install diracos2 (not strictly needed as can generate locally and copy across)
- install what's need for dask dashboard

Running dask
------------

To avoid have to define a new jobqueue name (config_name), I've just set it to htcondor which needs core and memory to
be set.

.. code-block:: python

    from dask_dirac import DiracCluster
    cluster = DiracCluster(cores=1, memory='0.5GB')
    cluster.scale(jobs=5)

    from dask.distributed import Client
    client = Client(cluster)
