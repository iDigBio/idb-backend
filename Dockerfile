FROM python:2.7
RUN set -ex && \
    apt-get update && \
    apt-get install -y libblas-dev liblapack-dev libatlas-base-dev \
            gfortran libgdal-dev libpq-dev libgeos-c1 libfontconfig1-dev \
            libxml2 libxslt1-dev libav-tools --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*
COPY ./requirements.txt /tmp/idb-requirements.txt
COPY ./idigbio_ingestion/requirements.txt /tmp/idigbio_ingestion-requirements.txt
RUN   pip --no-cache-dir install -r /tmp/idb-requirements.txt \
   && pip --no-cache-dir install -r /tmp/idigbio_ingestion-requirements.txt
COPY . /opt/idb-backend/
RUN pip install -e /opt/idb-backend/
