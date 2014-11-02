#!/usr/bin/env python

import json
import sys
import subprocess
import os.path

if len(sys.argv) < 3:

	print >>sys.stderr, "Usage: gather_worker_logs.py cluster.json target_dir"

cluster_file = sys.argv[1]
target_dir = sys.argv[2]

with open(cluster_file, "r") as f:
	cluster = json.load(f)

procs = []

for node in cluster:

	target_file = os.path.join(target_dir, "snf-%d.log" % node["id"])
	procs.append(subprocess.Popen(["/usr/bin/scp", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null", "%s:/home/user/jc-metrics.log" % node["ip"], target_file]))

for proc in procs:
	print "Wait for", proc
	proc.wait()

