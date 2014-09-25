#!/usr/bin/python

import time
import simplepost
import json
import sys

tries = 20
delay = 0.5
host = "localhost"
port = 8080

lastexn = None

for i in range(tries):

    try:

        response = simplepost.post(host, port, "/ping", {"echo": "hello"})
        if json.loads(response.read())["echo"] != "hello":
            raise Exception("Bad response: " + response)

        sys.exit(0)

    except Exception as e:

        lastexn = e
        time.sleep(delay)

print >>sys.stderr, "%s:%d did not appear after %d tries interval %g" % (host, port, tries, delay)
print >>sys.stderr, "Last exception:"
print >>sys.stderr, e
sys.exit(1)

