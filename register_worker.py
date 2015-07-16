#!/usr/bin/python

import simplepost
import socket
import sys
import json
import multiprocessing
import os

if len(sys.argv) < 2:
    print >>sys.stderr, "Usage: register_worker.py sched-address"
    sys.exit(1)

mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
mem_gib = int(round(mem_bytes/(1024.**3)))

cores = multiprocessing.cpu_count()

me = socket.getfqdn()
response = simplepost.post(sys.argv[1], 8080, "/addworker", {"address": me, "cores": str(cores), "memory": str(mem_gib)})

response_doc = json.load(response)

sys.stdout.write(str(response_doc["wid"]))
