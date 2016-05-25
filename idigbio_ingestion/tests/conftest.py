import pytest

@pytest.fixture(scope="session", autouse=True)
def logger():
    from idb.helpers.logging import idblogger, configure_app_log
    configure_app_log(verbose=2)
    from idb import config
    idblogger.debug("Test Env: %s", config.ENV)
    return idblogger.getChild('tests')
