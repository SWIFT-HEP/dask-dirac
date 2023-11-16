FROM quay.io/centos/centos:stream9

RUN dnf install -y epel-release \
 && dnf update -yq \
 && dnf install -y gcc openssl-devel bzip2-devel libffi-devel zlib-devel wget make \
 && dnf clean all

# Install python
RUN wget https://www.python.org/ftp/python/3.10.9/Python-3.10.9.tgz \
 && tar -xf Python-3.10.9.tgz \
 && cd Python-3.10.9 \
 && ./configure --enable-optimizations \
 && make -j 8 \
 && make install \
 && cd .. \
 && rm -fr Python-3.10.9.tgz Python-3.10.9

RUN ln -s /usr/bin/python3 /usr/bin/python
# install pip using get-pip.py
RUN wget https://bootstrap.pypa.io/get-pip.py \
 && python get-pip.py \
 && rm get-pip.py

# Install gfal
RUN dnf install -y gfal2-util gfal2-all \
 && dnf clean all

# Install python bits
RUN pip install --no-cache-dir dask==2023.3.1 distributed==2023.3.1

# run some tests
RUN python --version \
 && pip --version \
 && gfal-copy --version
