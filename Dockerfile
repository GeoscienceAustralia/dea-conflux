FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4 AS builder

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
      python3-pip \
      python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv

WORKDIR /build

COPY requirements.txt constraints.txt /conf/
RUN pip install --no-cache-dir \
      -r /conf/requirements.txt \
      -c /conf/constraints.txt

# The Git metadata is used by setuptools-scm to generate the package version.
COPY . /build
RUN echo "Installing dea-conflux through the Dockerfile." && \
    pip install --no-cache-dir . -c /conf/constraints.txt && \
    pip freeze && \
    pip check && \
    dea-conflux --version


FROM ghcr.io/osgeo/gdal:ubuntu-small-3.11.4 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH="/opt/venv/bin:$PATH"

# Retain the packages previously available at runtime, excluding build-only
# dependencies installed in the builder stage.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      fish \
      git \
      htop \
      libpq5 \
      python3 \
      unzip \
      vim \
      wget \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

RUN mkdir -p /code && \
    git config --global --add safe.directory /code && \
    pip check && \
    dea-conflux --version

WORKDIR /code
