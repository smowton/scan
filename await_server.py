#!/usr/bin/python

import time
import simplepost
import json
import sys

def await_server(host, port, tries, delay):

    lastexn = None

    for i in range(tries):

        try:

            response = simplepost.post(host, port, "/ping", {"echo": "hello"})
            if json.loads(response.read())["echo"] != "hello":
                raise Exception("Bad response: " + response)

            return True

        except Exception as e:

            lastexn = e
            time.sleep(delay)

    print >>sys.stderr, "%s:%d did not appear after %d tries interval %g" % (host, port, tries, delay)
    print >>sys.stderr, "Last exception:"
    print >>sys.stderr, e
    return False

if __name__ == "__main__":
    
    tries = 20
    delay = 0.5
    host = "localhost"
    port = 8080

    if await_server(host, port, tries, delay):
        sys.exit(0)
    else:
        sys.exit(1)

