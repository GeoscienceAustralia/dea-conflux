FROM osgeo/gdal:ubuntu-small-3.6.3

ENV SHELL=bash

ENV DEBIAN_FRONTEND=non-interactive

# Update sources list.
RUN apt clean && apt update \
  # Install basic tools for developer convenience.
  && apt install -y \
    curl \
    git \
    tmux \ 
    unzip \
    vim  \
  # Install pip3.
  && apt install -y --fix-missing --no-install-recommends \
    python3-pip \
  && python -m pip install --upgrade pip \
  # For psycopg2
  && apt install -y libpq-dev \ 
  # For hdstats
    python3-dev \
    build-essential \
  # Clean up.
  && apt clean \
  && apt  autoclean \
  && apt autoremove \
  && rm -rf /var/lib/{apt,dpkg,cache,log}

# Install AWS CLI.
WORKDIR /tmp
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip
RUN ./aws/install

# Copy requirements.txt and install python packages from requirements.txt.
RUN mkdir -p /conf
COPY docker/requirements.txt /conf/
RUN pip install -r /conf/requirements.txt

# Copy source code.
RUN mkdir -p /code
WORKDIR /code
ADD . /code
# Install source code.
RUN echo "Installing deafrica-conflux through the Dockerfile."
RUN pip install --extra-index-url="https://packages.dea.ga.gov.au" .

RUN pip freeze && pip check

# Make sure it's working
RUN deafrica-conflux --version