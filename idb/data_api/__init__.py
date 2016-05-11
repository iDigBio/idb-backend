from __future__ import division, absolute_import, print_function

import os
import sys

import click

from flask_cli import pass_script_info

from idb.cli import cli


@cli.command('run-server', short_help='Runs the data api server')
@click.option('--host', '-h', default='127.0.0.1',
              help='The interface to bind to.')
@click.option('--port', '-p', default=19197,
              help='The port to bind to.')
@click.option('--reload/--no-reload', default=None,
              help='Enable or disable the reloader.  By default the reloader '
              'is active if debug is enabled.')
@click.option('--debugger/--no-debugger', default=None,
              help='Enable or disable the debugger.  By default the debugger '
              'is active if debug is enabled.')
@click.option('--eager-loading/--lazy-loader', default=None,
              help='Enable or disable eager loading.  By default eager '
              'loading is enabled if the reloader is disabled.')
@click.option('--debug/--no-debug',
              help='Enable or disable debug mode.',
              default=None)
@pass_script_info
def run_server(info, host, port, reload, debugger, eager_loading, debug):
    """Runs a local development server for the Flask application.
    This local server is recommended for development purposes only but it
    can also be used for simple intranet deployments.  By default it will
    not support any sort of concurrency at all to simplify debugging.

    The reloader and debugger are by default enabled if the debug flag of
    Flask is enabled and disabled otherwise.
    """
    info.app_import_path = 'idb.data_api.api:app'
    info.debug = debug
    from idb import config
    print("Server Start http://{0}:{1}/ ENV={2}".format(host, port, config.ENV),
          file=sys.stderr)
    if config.ENV in ('beta', 'prod'):
        from gevent.pool import Pool
        from gevent.wsgi import WSGIServer

        http_server = WSGIServer(('', 19197), info.load_app(), spawn=Pool(1000))
        http_server.serve_forever()
    else:

        from werkzeug.serving import run_simple
        from flask_cli import DispatchingApp
        if reload is None:
            reload = info.debug
        if debugger is None:
            debugger = info.debug
        if eager_loading is None:
            eager_loading = not reload

        app = DispatchingApp(info.load_app, use_eager_loading=eager_loading)

        # Extra startup messages.  This depends a but on Werkzeug internals to
        # not double execute when the reloader kicks in.
        if os.environ.get('WERKZEUG_RUN_MAIN') != 'true':
            # If we have an import path we can print it out now which can help
            # people understand what's being served.  If we do not have an
            # import path because the app was loaded through a callback then
            # we won't print anything.
            if info.app_import_path is not None:
                print(' * Serving Flask app "%s"' % info.app_import_path)
            if info.debug is not None:
                print(' * Forcing debug %s' % (info.debug and 'on' or 'off'))

        run_simple(host, port, app, use_reloader=reload,
                   use_debugger=debugger, threaded=False,
                   passthrough_errors=True)
