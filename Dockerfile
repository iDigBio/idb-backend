FROM python:2.7
RUN set -ex && \
    apt-get update && \
    apt-get install -y libblas-dev liblapack-dev libatlas-base-dev \
            gfortran libgdal-dev libpq-dev libgeos-c1  fonts-dejavu-core \
            libxml2 libxslt1-dev libav-tools libsystemd-dev --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
USER www-data
COPY . /opt/idb-backend/
WORKDIR /opt/idb-backend/
RUN pip --no-cache-dir install -r /opt/idb-backend/requirements.txt
