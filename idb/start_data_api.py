import os

if __name__ == '__main__':
    from data_api.api import app

    if "ENV" in os.environ and (os.environ["ENV"] == "beta" or os.environ["ENV"] == "prod"):
        from gevent.pool import Pool
        from gevent.wsgi import WSGIServer

        print "Server Start"
        http_server = WSGIServer(('', 19197), app, spawn=Pool(1000))
        http_server.serve_forever()

    else:
        print "Server Start"
        app.run(debug=True,port=19197)