import pytest
import gevent
import os
import sys
from importlib import reload

@pytest.fixture
def app(testdbpool, testdata, logger):
    try:
        import idb
        import idb.helpers
        import idb.helpers.logging
        from idb.data_api import api
        reload(api)
    except ImportError:
        print ("Failed import, %r %r" % (os.getcwd(), sys.path))
        logger.error("Failed import, %r %r", os.getcwd(), sys.path)
        raise

    app = api.app
    app.config['DB'] = testdbpool

    def cleanup(exception):
        logger.info("Cleanup app idbmodel")
        from idb.data_api.common import idbmodel
        idbmodel.rollback()
        idbmodel.close()
        gevent.wait()
    app.teardown_appcontext(cleanup)

    return app
