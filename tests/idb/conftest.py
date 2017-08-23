import pytest
import gevent
import os
import sys

@pytest.fixture
def app(testdbpool, testdata, logger):
    try:
        import idb
        import idb.helpers
        import idb.helpers.logging
        import logging
        from idb.data_api import api
        idb.helpers.logging.configure(stderr_level=logging.DEBUG)
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
