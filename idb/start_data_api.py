if __name__ == '__main__':
    from gevent.pool import Pool
    from gevent.wsgi import WSGIServer
    from data_api import app

    print "Server Start"
    http_server = WSGIServer(('', 19197), app, spawn=Pool(1000))
    http_server.serve_forever()

    #app.run(debug=True)