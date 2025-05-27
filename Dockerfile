FROM pypy:2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gfortran \
    libatlas-base-dev \
    ffmpeg \
    libblas-dev \
#    libgdal-dev \
    libgeos-dev  \
    liblapack-dev \
    libpq-dev \
    build-essential \
    make \
    autotools-dev \
    autoconf \
    automake \
    git \
    cmake \
    libcrypto++-dev \
    libproj-dev \
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
RUN git clone https://github.com/sqlite/sqlite.git
RUN cd sqlite && CFLAGS="-DSQLITE_ENABLE_COLUMN_METADATA=1" ./configure --rtree && /usr/bin/make && /usr/bin/make install && cd ..
RUN mkdir libgdal && wget https://download.osgeo.org/gdal/3.8.1/gdal-3.8.1.tar.gz && tar -xzf gdal-3.8.1.tar.gz && cd gdal-3.8.1 && \
mkdir build && cd build && cmake .. && make && make install
RUN cp /usr/local/lib/libgdal* /lib/
RUN GDAL_CONFIG=/usr/local/bin/gdal-config pip install --no-cache-dir --no-binary fiona fiona
RUN python -m build
RUN pip --no-cache-dir install -e .[test]
USER www-data
