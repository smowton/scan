#!/usr/bin/python

import simplepost
import socket
import sys
import json

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: register_worker.py sched-address worker-class"
    sys.exit(1)

me = socket.getfqdn()
response = simplepost.post(sys.argv[1], 8080, "/addworker", {"classname": sys.argv[2], "address": me})

response_doc = json.load(response)

sys.stdout.write(str(response_doc["wid"]))
