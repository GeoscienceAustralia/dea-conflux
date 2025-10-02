FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0

# Install system dependencies for Python and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential fish git vim htop wget unzip python3 python3-pip python3-venv python3-dev curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# The installer requires curl (and certificates) to download the release archive
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Adding uv as package manager based on https://devblogs.microsoft.com/ise/dockerizing-uv/
# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"
# Makes installation faster
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
RUN uv venv /opt/venv
# Use the virtual environment automatically
ENV VIRTUAL_ENV=/opt/venv
# Place entry points in the environment at the front of the path
ENV PATH="/opt/venv/bin:$PATH"
# Use uv to install dependencies
RUN mkdir -p /conf
COPY requirements.txt /conf/
COPY constraints.txt /conf/
RUN uv pip install -r /conf/requirements.txt -c /conf/constraints.txt

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN echo "Installing dea-conflux through the Dockerfile."
RUN uv pip install -e . -c /conf/constraints.txt

RUN uv pip freeze && uv pip check

# Make sure it's working
RUN dea-conflux --version
