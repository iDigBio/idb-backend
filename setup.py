import os
import re
from setuptools import setup, find_packages

from codecs import open


def read(*paths):
    """Build a file path from *paths* and return the contents."""
    with open(os.path.join(*paths), 'r', 'utf-8') as f:
        return f.read()

readme = read('README.md')

version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                    read(os.path.join(os.path.dirname(__file__), 'idb/__init__.py')), re.MULTILINE).group(1)

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
    install_requires=[
        'psycopg2>=2.6',
        'redis>=2.9.1, <3.0.0',
        'python-dateutil>=2.2, <3.0',
        'elasticsearch>=2.3, <3',
        'pyproj>=1.9.3',
        'pytz>=2016.4',
        'requests>=2.4.0',
        'pycrypto',
        'flask>=0.11.0, <1.0.0',
        'Flask-UUID',
        'Flask-CORS',
        'coverage',
        'numpy',
        'scipy',
        'gevent>=1.1.0, <1.2.0',
        'gipc>=0.6.0, <0.7.0',
        'unicodecsv>=0.14.1, < 0.15.0',
        'shapely',
        'celery',
        'boto>=2.39.0, <3.0.0',
        'fiona',
        'python-magic>=0.4.11, <=0.5.0',
        'feedparser>=5.2.0',
        'click>=6.3, <7.0',
        'atomicfile==1.0',
        'enum34>=1.1.6, <1.2.0',
        'pytest-runner',
    ],
    extras_require={
        'journal': ['python-systemd>=230'],
        'ingestion': [
            'pydub==0.16.5',
            'pillow>=3.2.0',
            'Python-fontconfig',
            'lxml',
            'chardet',
            'pyquery>=1.2',
        ],
        'test': [
            'pytest>=2.6',
            'pytest-cov',
            'pytest-flask',
            'pytest-mock',
            'pytest-capturelog'
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
