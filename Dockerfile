FROM python:3.8.10
# python:3.6 and later are based on "Debian GNU/Linux"
RUN apt-get update && apt-get install -y --no-install-recommends \
    gfortran \
    libatlas-base-dev \
    ffmpeg \
    libblas-dev \
    libgdal-dev \
#    libgeos-c1  \
    liblapack-dev \
    libpq-dev \
#    libsystemd-dev   \
#    fonts-dejavu-core \
#    libxml2 \
#    libxslt1-dev \
  && rm -rf /var/lib/apt/lists/*

COPY . /opt/idb-backend/
WORKDIR /opt/idb-backend/
RUN pip --no-cache-dir install -e .[test]
USER www-data
