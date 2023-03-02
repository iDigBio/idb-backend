FROM python:2.7

RUN apt-get update; \
    apt-get install -y --no-install-recommends \
        gfortran \
        libatlas-base-dev \
        ffmpeg \
        libblas-dev \
        libgdal-dev \
        liblapack-dev \
        libpq-dev \
        bsdmainutils; \
    rm -rf /var/tmp/* /tmp/* /var/lib/apt/lists/*

RUN /usr/local/bin/python -m pip install --upgrade pip

# Install the list of packages in setup.py here so that this
# image layer can be cached.  It takes a long time to build and
# install these.
COPY ./requirements.txt /opt/idb-backend/requirements.txt
WORKDIR /opt/idb-backend
RUN pip --no-cache-dir install -r requirements.txt