FROM python:2.7
# base image 'python:2.7' is based on 'debian:buster-20200414',
# whose repository has moved to archive.debian.org
RUN sed --in-place --regexp-extended \
    's/http:\/\/(deb|security).debian.org/http:\/\/archive.debian.org/g' \
    /etc/apt/sources.list
RUN apt-get -y update && apt-get install -y --no-install-recommends \
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
