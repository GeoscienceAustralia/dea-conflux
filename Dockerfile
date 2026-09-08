FROM ghcr.io/osgeo/gdal:ubuntu-small-3.12.4 AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH="/opt/venv/bin:$PATH"

# Keep compilers, development headers, and venv tooling out of the runtime image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      libpq-dev \
      python3 \
      python3-dev \
      python3-pip \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv

WORKDIR /build

COPY requirements.txt constraints.txt /conf/
RUN pip install --no-cache-dir --no-compile \
      -r /conf/requirements.txt \
      -c /conf/constraints.txt

# The Git metadata is used by setuptools-scm to generate the package version.
COPY . /build
RUN echo "Installing dea-conflux through the Dockerfile." && \
    pip install --no-cache-dir --no-compile . -c /conf/constraints.txt && \
    pip freeze && \
    pip check && \
    dea-conflux --version
    
WORKDIR /code
