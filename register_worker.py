#!/usr/bin/python

import simplepost
import socket
import sys
import json
import multiprocessing
import os
import subprocess

if len(sys.argv) < 2:
    print >>sys.stderr, "Usage: register_worker.py sched-address"
    sys.exit(1)

mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
mem_gib = int(round(mem_bytes/(1024.**3)))

cores = multiprocessing.cpu_count()

sched_ip4 = socket.gethostbyname(sys.argv[1])
route_report = subprocess.check_output(["/sbin/ip", "route", "get", sched_ip4])
lines = route_report.split("\n")
if len(lines) == 0:
    print >>sys.stderr, "IP route query for", sched_ip4, "returned", route_report
    sys.exit(1)

bits = lines[0].split()

try:
    me = bits[bits.index("src") + 1]
except:
    print >>sys.stderr, "Unexpected format for IP route query", sched_ip4, ":", route_report

response = simplepost.post(sys.argv[1], 8080, "/addworker", {"address": me, "cores": str(cores), "memory": str(mem_gib)})

response_doc = json.load(response)

sys.stdout.write(str(response_doc["wid"]))
