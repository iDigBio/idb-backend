import idb.config
import pytest


def test_idb_clibase():
    "Pretty much just import the module to check for syntax/crazy stuff"
    from idb import clibase


def test_idb_cli():
    "Pretty much just import the module to check for syntax/crazy stuff"
    from idb import cli


def test_std_options(monkeypatch):
    from idb.clibase import handle_std_options
    env = idb.config.ENV
    handle_std_options()
    handle_std_options(verbose=1)
    handle_std_options(env="test")
    assert idb.config.ENV == "test"
    idb.config.ENV = env
