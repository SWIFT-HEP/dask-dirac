FROM continuumio/miniconda3

WORKDIR /app

SHELL ["/bin/bash", "--login", "-c"]

# Create the environment:
COPY environment.yaml .
RUN conda env create -f environment.yaml

# Make RUN commands use the new environment:
RUN conda init bash
RUN conda activate dask_dirac

CMD ["/bin/bash"]
