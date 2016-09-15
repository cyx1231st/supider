import eventlet
eventlet.monkey_patch()

from eventlet import GreenPool
import httplib
import socket
import traceback
import urllib2


class Fail(Exception):
    pass
class Error(Exception):
    pass


class Connector(object):
    def __init__(self,
                 timeout_retries=2,
                 timeout=20,
                 concurrency=2):
        self._timeout_retries = timeout_retries
        self._timeout = timeout
        self._connect_pool = None
        self.connections = 0

        self.fail = False

        self.reset(concurrency)

    @property
    def connections(self):
        return self._connections

    @connections.setter
    def connections(self, val):
        self._connections = val

    def reset(self, pool_size):
        self.fail = False
        self._connect_pool = GreenPool(pool_size)

    def _connect_protected(self, url):
        req = urllib2.Request(url)

        source_code = None
        for i in range(0, self._timeout_retries):
            if self.fail:
                return Fail()
            try:
                source_code = urllib2.urlopen(req, timeout=self._timeout).read()
            # except (httplib.BadStatusLine, urllib2.URLError) as e:
            #     # connection timeout, retry
            #     pass
            # except (httplib.IncompleteRead, socket.timeout) as e:
            #     self.fail = True
            #     return Fail()
            except Exception as e:
                do_retry = False
                if isinstance(e, urllib2.URLError):
                    if "[Errno 8]" in str(e):
                        self.fail = True
                        return Fail("Connection broke(URLError 8)")
                    elif "urlopen error timed out" in str(e):
                        do_retry = True
                    elif "[Errno 50]" in str(e):
                        self.fail = True
                        return Fail("Connection broke(URLError 50)")
                elif isinstance(e, socket.timeout):
                    if "timed out" in str(e):
                        do_retry = True
                elif isinstance(e, socket.error):
                    if "[Errno 60] Operation timed out" in str(e):
                        do_retry = True
                if not do_retry:
                    msg = str(type(e)) + "\n"
                    msg += traceback.format_exc()
                    self.fail = True
                    return Error(msg)
            else:
                break
        if source_code is None:
            self.fail = True
            return Fail("Retry exceeded %s" % self._timeout_retries)

        return source_code

    def _connection_counter(self, url):
        ret = self._connect_protected(url)
        return ret

    def connect(self, url, async=True):
        if async:
            connect_thread = self._connect_pool.spawn(self._connection_counter, url)
            result = connect_thread.wait()
        else:
            result = self._connect_protected(url)
        return result
