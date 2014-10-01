#!/usr/bin/python

import simplepost
import socket
import sys

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: register_worker.py sched-address worker-class"
    sys.exit(1)

me = socket.getfqdn()
simplepost.post(sys.argv[1], 8080, "/addworker", {"classname": sys.argv[2], "address": me})
 
