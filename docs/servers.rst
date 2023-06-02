Sam's Corner
============

Steps;

1. Submit to certification server
2. Submit to dev server
3. Production


Certification Server
-------------------

Setup DIRAC UI

.. code-block:: sh

    mkdir dirac_ui
    cd dirac_ui
    curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh
    bash DIRACOS-Linux-$(uname -m).sh
    rm DIRACOS-Linux-$(uname -m).sh
    source diracos/diracosrc
    pip install DIRAC
    dirac-proxy-init -x -N
    dirac-configure


Test submission

.. code-block:: sh

    dask-dirac whoami https://lbcertifdirac70.cern.ch:8443 --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates --user-proxy /tmp/x509up_u1000


Development Server
------------------

Setup DIRAC UI

.. code-block:: sh

    mkdir dirac_ui
    cd dirac_ui
    curl -LO https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-$(uname -m).sh
    bash DIRACOS-Linux-$(uname -m).sh
    rm DIRACOS-Linux-$(uname -m).sh
    source diracos/diracosrc
    pip install DIRAC
    dirac-proxy-init -x -N
    dirac-configure -F -S GridPP -C dips://diracdev.grid.hep.ph.ic.ac.uk:9135/Configuration/Server -I


Test submission

.. code-block:: sh

    dask-dirac whoami https://diracdev.grid.hep.ph.ic.ac.uk:8443 --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates --user-proxy /tmp/x509up_u1000

now works
