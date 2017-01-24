FROM python:2.7
RUN apt-get update && apt-get install -y --no-install-recommends \
    fonts-dejavu-core \
    gfortran \
    libatlas-base-dev \
    libav-tools \
    libblas-dev \
    libgdal-dev \
    libgeos-c1  \
    liblapack-dev \
    libpq-dev \
    libsystemd-dev   \
    libxml2 \
    libxslt1-dev \
  && rm -rf /var/lib/apt/lists/*

USER www-data
COPY . /opt/idb-backend/
WORKDIR /opt/idb-backend/
RUN pip --no-cache-dir install -r /opt/idb-backend/requirements.txt
