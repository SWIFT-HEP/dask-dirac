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
    cluster = DiracCluster(cores=1, memory='0.5GB', scheduler_options={"port":8786})
    cluster.scale(jobs=5)

    from dask.distributed import Client
    client = Client(cluster)



Instructions from scratch
=========================

pre-reqs... ports to be open


Setup proxy

.. code-block:: bash

    curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-x86_64.sh
    bash DIRACOS-Linux-x86_64.sh

    #
    source diracos2/diracos
    pip install DIRAC

    dirac-proxy-init --nocs
    dirac-configure
    dirac-proxy-init -g dteam_user


Now get dask_dirac

.. code-block:: bash

    git clone git@github.com:SWIFT-HEP/dask-dirac.git
    cd dask-dirac
    pip install .

Now test it out

.. code-block:: python

    from dask_dirac import DiracCluster
    cluster = DiracCluster(cores=1, memory='0.5GB', scheduler_options={"port":8786})
    cluster.scale(jobs=5)

    from dask.distributed import Client
    client = Client(cluster)


DiracCluster options are;

- submission_url="https://lbcertifdirac70.cern.ch:8443"
- user_proxy="/tmp/x509up_u1000"
- cert_path="/etc/grid-security/certificates"
- jdl_file=os.getcwd() + "/grid_JDL"
