# taskletproc.py  (put next to the module that used gipc)

import traceback
from stackless import channel, tasklet, run as schedule_run

class TaskletProc(object):
    """
    Very small stand-in for a gipc Process.
    - Call .start(target, *args, **kw) to begin.
    - .join() blocks until the tasklet finishes.
    - .exitcode == 0 on success, 1 on uncaught exception.
    """

    def __init__(self, name=u"tasklet"):
        self.name = name
        self._done = channel()
        self.exitcode = None
        self._t = None

    # --------------------------------------------------
    # API similar to multiprocessing.Process
    # --------------------------------------------------
    def start(self, target, *args, **kw):
        def _runner():
            try:
                rc = target(*args, **kw) or 0
                self.exitcode = int(rc)
            except BaseException:
                traceback.print_exc()
                self.exitcode = 1
            finally:
                self._done.send(None)          # notify joiners
        self._t = tasklet(_runner)()

    def join(self):
        """Block until the tasklet finishes (acts like .join())."""
        self._done.receive()

    # no terminate() - if you need it you can add a ‘poison’ flag
