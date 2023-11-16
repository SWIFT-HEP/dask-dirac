FROM python:3.10.9-slim-bullseye
#FROM python:3.10.13-slim-bookworm

SHELL ["/bin/bash", "-c"]

# Install gfal2
#RUN apt-get -y update && apt-get install -y gfal2

#RUN apt-get install -y python3-gfal2 python3-gfal2-util gfal2-util-scripts

RUN pip3 install --no-cache-dir dask distributed

# gfal is in RHEL bias
#RUN ln -s /usr/local/bin/python /usr/bin/python
