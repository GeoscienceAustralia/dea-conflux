ARG GDAL_IMAGE=ghcr.io/osgeo/gdal:ubuntu-small-3.12.4
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.8.8

FROM ${UV_IMAGE} AS uv

FROM ${GDAL_IMAGE} AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    UV_COMPILE_BYTECODE=0 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH="/opt/venv/bin:$PATH"

# Keep compilers, development headers, and venv tooling out of the runtime image.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      git \
      python3 \
      python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv

RUN uv venv --python /usr/bin/python3 /opt/venv

WORKDIR /build

COPY pyproject.toml uv.lock /build/
RUN uv sync --locked --no-dev --no-install-project

# The Git metadata is used by setuptools-scm to generate the package version.
COPY . /build
RUN echo "Installing dea-conflux through the Dockerfile." && \
    uv sync --locked --no-dev --no-editable && \
    uv pip check --python /opt/venv/bin/python && \
    PYTHONDONTWRITEBYTECODE=1 dea-conflux --version && \
    find /opt/venv/lib -type f -name '*.pyc' -delete && \
    find /opt/venv/lib -type d \( -name test -o -name tests \) -prune -exec rm -rf '{}' +


FROM ${GDAL_IMAGE} AS runtime-base

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    PATH="/opt/venv/bin:$PATH"

COPY --from=builder /opt/venv /opt/venv

FROM runtime-base AS test

# Git and uv are only required by the Compose-based test workflow, which syncs
# the mounted checkout with its test extra.
ENV UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      git \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv

RUN mkdir -p /code && \
    git config --global --add safe.directory /code

WORKDIR /code

FROM runtime-base AS runtime

WORKDIR /code