Developer's Corner
==================

Development server
------------------

As discussed on `GitHub https://github.com/DIRACGrid/DIRAC/discussions/6121`_, there is a development server available for DIRAC.
The server has some resources behind it and requires a manual registration to to the CS/Registry of `lbcertifdirac70.cern.ch`.
The developer needs to be a member of the dteam Virtual Organisation (VO) to be able to use the server.


Production server
----------------
The `production server https://dirac.gridpp.ac.uk:8443/DIRAC/`_ is not yet ready to accept HTML requests.
The necessary upgrade is on the roadmap and this document will be updated when the upgrade is complete.


Testing
-------

```bash
dask-dirac whoami https://lbcertifdirac70.cern.ch:8443 --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates --user-proxy /tmp/x509up_u1000
```

Job submission
JDL:
```python
JobName = "DASK_DIRAC";
Executable = "/bin/ls";
Arguments = "-ltrA";
StdOutput = "std.out";
StdError = "std.err";
OutputSandbox = {"std.out","std.err"};
```

```bash
dask-dirac submit https://lbcertifdirac70.cern.ch:8443 \
    --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates \
    --user-proxy /tmp/x509up_u1000 \
    --jdl-file jdl.txt
```

Job status
```bash
dask-dirac status https://lbcertifdirac70.cern.ch:8443 \
    --capath /cvmfs/grid.cern.ch/etc/grid-security/certificates \
    --user-proxy /tmp/x509up_u1000 \
    123456789
```
