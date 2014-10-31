#!/usr/bin/env python

import json
import sys
import subprocess

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: okeanos_modify_cluster.py cluster_machines.json new_flavour_id"
	sys.exit(1)

with open(sys.argv[1], "r") as f:
	machines = json.load(f)

for machine in machines:
        subprocess.call(["kamaki", "server", "modify", "--flavor-id", sys.argv[2], str(machine["id"])])


