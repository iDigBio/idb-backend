FROM python:2.7
RUN set -ex && \
    apt-get update && \
    apt-get install -y libblas-dev liblapack-dev libatlas-base-dev \
            gfortran libgdal-dev libpq-dev libgeos-c1 libfontconfig1-dev \
            libxml2 libxslt1-dev libav-tools libsystemd-dev --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
COPY . /opt/idb-backend/
RUN cd /opt/idb-backend && pip --no-cache-dir install -r /opt/idb-backend/requirements.txt /opt/idb-backend/[ingestion]
