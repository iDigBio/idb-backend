import os
import re
import sys
from setuptools import setup, find_packages
from setuptools.extension import Extension
from Cython.Build import cythonize

from codecs import open

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r', 'utf-8') as f:
        return f.read()

readme = read('README.md')

version = "3.0.8"

# Abort if on Python 3 since we know the codebase does not support it.
#if sys.version_info >= (3,0):
    #sys.exit("idb-backend: Python 3 is not supported. Consider passing '-p python2.7' when creating the venv.")

# Pillow-SIMD causes segfault on newer Python 2.
# We do not yet support Python 3.
# Only install Pillow-SIMD where it is known to work, otherwise
# install Pillow.
# (see https://github.com/iDigBio/idb-backend/issues/92)
""" if sys.version_info >= (2,7,15):
    pillow_package = "pillow>=3.4,<=5.1.1"
else:
    pillow_package = "pillow-simd>=3.4,<=9.5.0.post2" """

# Extension modules
extensions = [
    Extension("idb.cli", ["idb/cli.py"]),
    Extension("idb.corrections.record_corrector", ["idb/corrections/record_corrector.py"]),
    Extension("idb.indexing.index_from_postgres", ["idb/indexing/index_from_postgres.py"]),
    Extension("idb.indexing.__init__", ["idb/indexing/__init__.py"]),
    Extension("idb.helpers.etags", ["idb/helpers/etags.py"]),
    Extension("idb.helpers.storage", ["idb/helpers/storage.py"]),
    Extension("idigbio_ingestion.cli", ["idigbio_ingestion/cli.py"]),
    Extension("idigbio_ingestion.db_check", ["idigbio_ingestion/db_check.py"]),
]

setup(
    name='idb-backend',
    version=version,
    description='Python backend for iDigBio services',
    long_description=readme,
    url='http://github.com/idigbio/idb-backend/',
    license='MIT',
    author='ACIS iDigBio team',
    author_email='idigbio@acis.ufl.edu',
    packages=find_packages(exclude=['tests*']),
    ext_modules=cythonize(extensions, compiler_directives={'language_level': '3','always_allow_keywords': True}),
    setup_requires=['pytest-runner','cython'],
    #setup_requires=['pytest-runner'],
    install_requires=[
        'Cython<3.0',
        'idigbio>=0.8.2',
        'psycopg2cffi>=2.9.0',
        'psycopg2>=2.9.9',
        'redis>=2.9.1',
        'python-dateutil>=2.2, <3.0',
        'udatetime>=0.0.13',
        'elasticsearch>=5, <6',
        'pyproj>1.9.3',
        'pytz>=2016.10',
        'requests>=2.31.0',
        'urllib3>1.25.11',
        'pycryptodome<4',
        'flask>=2.2.5',
        'Flask-UUID',
        'Flask-CORS',
        'coverage',
        'numpy',
 #       'scipy<=1.2.3',
        'gevent>=23.9.0',
        'gipc>=0.6.0, <0.7.0',
        'unicodecsv>=0.14.1, < 0.15.0',
        'shapely',
        'celery[redis]>=5.2.2',
        #'boto>=2.49.0, <3.0.0',
        'boto3>1.17.112',
        'fiona>1.8.22',
        'python-magic>=0.4.11, <=0.5.0',
        'feedparser>=5.2.0',
        'click>=6.3',
        'atomicfile==1.0',
        'enum34>=1.1.6, <1.2.0',
        'path.py>=10.0.0, <11',
        'wsgi-request-logger>=0.4.6',
        'jsonlines>=1.1.3',
        'py4j==0.10.9.7',
        'lxml==5.0.2',
        'bsddb3==6.2.9',
        "pillow-simd==9.5.0.post2",
        'pyquery>1.2.17',
        'pydub==0.16.5',
    ],
    extras_require={
        'ingestion': [
            #pillow_package,
            'lxml',
            'chardet==3.0.4',
        ],
        'test': [
            'pytest>=3.0',
            'pytest-cov',
            'pytest-flask',
            'pytest-mock==1.13.0',
            'fakeredis',
        ]
    },
    tests_require=['idb-backend[test]'],
    include_package_data=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Natural Language :: English',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
    entry_points='''
        [console_scripts]
        idb=idb.cli:cli
        idigbio-ingestion=idigbio_ingestion.cli:cli
    '''
)
