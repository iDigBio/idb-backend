FROM pypy:2
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
RUN python -m ensurepip
RUN python -mpip install -U pip wheel
RUN python -mpip install pygments
RUN pip install -U pip build setuptools
RUN python -m build
RUN pip --no-cache-dir install -e .[test]
USER www-data
