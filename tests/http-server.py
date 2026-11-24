import os
import signal
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

listen_after, fail_after = map(float, sys.argv[1:])
# Bound the fixture's lifetime even if the wrapper fails to reap it.
signal.alarm(10)
time.sleep(listen_after)
fail_at = time.monotonic() + fail_after


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200 if time.monotonic() < fail_at else 503)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        pass


with HTTPServer(("127.0.0.1", int(os.environ["TEST_HTTP_PORT"])), Handler) as server:
    server.serve_forever()
