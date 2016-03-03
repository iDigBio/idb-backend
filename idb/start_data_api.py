from __future__ import absolute_import
import os

if __name__ == '__main__':
    from idb.data_api.api import app

    env = os.environ.get('ENV', 'dev')
    if env in ('beta', 'prod'):
        from gevent.pool import Pool
        from gevent.wsgi import WSGIServer

        print "Server Start"
        http_server = WSGIServer(('', 19197), app, spawn=Pool(1000))
        http_server.serve_forever()

    else:
        print "Server Start"
        app.run(debug=True, port=19197)
