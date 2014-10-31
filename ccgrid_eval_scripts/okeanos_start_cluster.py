#!/usr/bin/env python

import json
import sys
import subprocess

if len(sys.argv) < 2:
	print >>sys.stderr, "Usage: okeanos_destroy_cluster.sh cluster_machines.json"
	sys.exit(1)

with open(sys.argv[1], "r") as f:
	machines = json.load(f)

for machine in machines:
        subprocess.check_call(["kamaki", "server", "start", str(machine["id"])])


