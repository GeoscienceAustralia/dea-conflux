FROM osgeo/gdal:ubuntu-small-3.10.0

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

# Apt installation
RUN apt-get update && \
    apt-get install -y \
      build-essential \
      fish \
      git \
      vim \
      htop \
      wget \
      unzip \
      python3-pip \
      libpq-dev python-dev \
    && apt-get autoclean && \
    apt-get autoremove && \
    rm -rf /var/lib/{apt,dpkg,cache,log}

# Pip installation
# RUN pip install --upgrade pip==23.1 setuptools==59.7.0
RUN mkdir -p /conf
COPY requirements.txt /conf/
COPY constraints.txt /conf/
RUN pip install -r /conf/requirements.txt -c /conf/constraints.txt
RUN pip install --upgrade pip==23.1 setuptools==59.7.0

# Copy source code and install it
RUN mkdir -p /code
WORKDIR /code
ADD . /code

RUN echo "Installing dea-conflux through the Dockerfile."
RUN pip install . -c /conf/constraints.txt

RUN pip freeze && pip check

# Make sure it's working
RUN dea-conflux --version
