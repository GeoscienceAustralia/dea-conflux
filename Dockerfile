FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8
RUN apt-get update && \
    apt-get install -y python3-pipx ... && \
    pipx install uv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set pipx binaries in path
ENV PATH="/root/.local/bin:${PATH}"

# Use uv to install dependencies
COPY constraints.txt /conf/
COPY requirements.txt /conf/
RUN uv pip install -r /conf/requirements.txt -c /conf/constraints.txt

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN echo "Installing dea-conflux through the Dockerfile."
RUN uv pip install . -c /conf/constraints.txt

RUN uv pip freeze && uv pip check

# Make sure it's working
RUN dea-conflux --version
