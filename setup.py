from os import path
import re
import sys
from setuptools import setup, find_packages

def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(path.join(*paths), 'r', encoding='utf-8') as f:
        return f.read()

readme = read('README.md')

version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(path.join(path.dirname(__file__), 'idb/__init__.py')), re.MULTILINE).group(1)

# Abort if not on Python 3 since we know the codebase does not support it.
if sys.version_info < (3,0):
    sys.exit("idb-backend: Python 2 is not supported in this branch.")

# TODO: revisit this after migrate to Python 3
# Pillow-SIMD causes segfault on newer Python 2.
# We do not yet support Python 3.
# Only install Pillow-SIMD where it is known to work, otherwise
# install Pillow.
# (see https://github.com/iDigBio/idb-backend/issues/92)
if sys.version_info >= (2,7,15):
    pillow_package = "pillow>=3.4,<=5.1.1"
else:
    pillow_package = "pillow-simd>=3.4,<=5.1.1"

    # setup_requires=["cffi>=1.0.0"],
    # cffi_modules=["package/foo_build.py:ffibuilder"],
    # install_requires=["cffi>=1.0.0"],

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
    setup_requires=['pytest-runner'],
    #    setup_requires=["pytest-runner","cffi>=1.0.0"],
    install_requires=[
        'idigbio>=0.8.2',
        'psycopg2-binary>=2.8.3',
        'redis>=2.9.1, <3.0.0',
        'python-dateutil>=2.2, <3.0',
        'udatetime>=0.0.13',
        'elasticsearch>=5, <6',
        'pyproj>=1.9.3',
        'pytz>=2016.10',
        'requests==2.20.0',
        'urllib3<1.25,>=1.21.1',
        'pycrypto',
        'flask==1.1.2',
        'Flask-UUID',
        'Flask-CORS',
        'coverage',
        'numpy',
        'scipy',
        'gevent==1.3.0',
        'gipc>=0.6.0, <0.7.0',
        'unicodecsv>=0.14.1, < 0.15.0',
        'shapely',
        'celery[redis]>=4.0, <4.3',
        'boto>=2.39.0, <3.0.0',
        'fiona==1.8.20',
        'python-magic>=0.4.11, <=0.5.0',
        'feedparser>=5.2.0',
        'click>=6.3, <7.0',
        'atomicfile==1.0',
        'enum34>=1.1.6, <1.2.0',
        'path.py>=10.0.0, <11',
        'wsgi-request-logger>=0.4.6',
        'jsonlines>=1.1.3',
        'wheel',
        'itsdangerous==2.0.1',
        'jinja2<3.1.0',
        'werkzeug==2.0.3'
    ],
    extras_require={
        'ingestion': [
            'pydub==0.16.5',
            'Pillow==6.1.0',
            'lxml',
            'chardet~=3.0',
            'pyquery>=1.2',
        ],
        'test': [
            'pytest~=4.0',
            'pytest-cov',
            'pytest-flask~=0.15',
            'pytest-mock==1.13.0',
            'fakeredis',
            'greenlet==2.0.2'
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
        'Programming Language :: Python :: 3.6',
    ],
    entry_points='''
        [console_scripts]
        idb=idb.cli:cli
        idigbio-ingestion=idigbio_ingestion.cli:cli
    '''
)
