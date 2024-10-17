JDL Examples
============

JDL submission
------------------

.. code-block:: sh

    dask-dirac submit https://lbcertifdirac70.cern.ch:8443 \
    --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates \
    --user-proxy /tmp/x509up_u1000 \
    JDL


JDLs
----

JDL-1
- submit :code:`/bin/ls` job

JDL-2
- submit :code:`/bin/ls` job to a defined Site

JDL-3
- submit :code:`singularity` job to run :code:`docker hello-world`

JDL-4
- submit :code:`singularity` job to run dask docker container (debian)
