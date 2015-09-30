#!/usr/bin/python

# Run a script on all remotes named in the cluster file, supplying their sudo password as the first argument.

import sys
import json
import subprocess
import tempfile
import threading
import shutil
import os.path

with open(sys.argv[1], "r") as f:
	machines = json.load(f)

tempdir = tempfile.mkdtemp()

ssh_cmd = ["/usr/bin/ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
scp_cmd = ["/usr/bin/scp"] + ssh_cmd[1:]

script = sys.argv[2]

lim = int(sys.argv[3]) if len(sys.argv) >= 4 else len(machines)

print "Writing output to", tempdir

run_threads = []

for m in machines[:lim]:

	"Start", m["ip"]
	
	def runm(m):

		with open(os.path.join(tempdir, m["ip"]), "w") as f:

			subprocess.check_call(scp_cmd + [script, "%s:/tmp/run-cluster" % m["ip"]], stdout=f, stderr=subprocess.STDOUT)
			subprocess.check_call(ssh_cmd + [m["ip"], "/tmp/run-cluster", m["passwd"]] + sys.argv[3:], stdout=f, stderr=subprocess.STDOUT)

	t = threading.Thread(target=runm, args=[m])
	t.start()
	run_threads.append((t, m["ip"]))

for (t, ip) in run_threads:

	print "Wait for", ip
	t.join()

#shutil.rmtree(tempdir)
