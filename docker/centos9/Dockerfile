FROM quay.io/centos/centos:stream9

RUN yum groupinstall -y "Development Tools"
RUN dnf install -y openssl-devel bzip2-devel libffi-devel

RUN dnf install -y wget

# Install python
RUN wget https://www.python.org/ftp/python/3.10.9/Python-3.10.9.tgz && \
    tar -xf Python-3.10.9.tgz && \
    cd Python-3.10.9 && \
    ./configure --enable-optimizations && \
    make -j 8 && \
    make install

# Install gfal
RUN dnf install -y epel-release
RUN dnf install -y gfal2-util gfal2-all
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install python bits
RUN dnf install -y python3-pip
RUN pip3 install --no-cache-dir dask==2023.3.1 distributed==2023.3.1
