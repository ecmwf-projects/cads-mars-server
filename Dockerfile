FROM continuumio/miniconda3

WORKDIR /src/cads-mars-server

COPY environment.yml /src/cads-mars-server/

RUN conda install -c conda-forge gcc python=3.11 \
    && conda env update -n base -f environment.yml

COPY . /src/cads-mars-server

RUN pip install --no-deps -e .
