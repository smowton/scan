#!/usr/bin/env python

# Keep a SCAN cluster busy: keep starting new jobs using the script given as a parameter
# keeping the worker utilistion for the first stage around 1. Waver a little around that figure,
# sometimes permitting some idleness and sometimes overloading a little.

import simplepost
import sys
import subprocess
import datetime
import time
import json
import random
import os
import signal

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: workgen_keep_busy.py start-job-script first-stage-queue-name job-directory-prefix"
	sys.exit(1)

scan_server = "localhost"
scan_port = 8080

start_job_script = sys.argv[1]
monitor_queue = sys.argv[2]
job_root = sys.argv[3]
temp_root = sys.argv[4]

running_procs = set()

next_job = 0

def shutdown(signum, frame):
	for proc, cmd in running_procs:
		try:
			print ts(), "Stop process", list(cmd)
			proc.terminate()
		except Exception as e:
			print ts(), "Failed to stop process", list(cmd), e

	sys.exit(1)

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

def ts():
	return datetime.datetime.now().isoformat()

def get_utilisation():

	while True:
		try:
			workers = json.load(simplepost.post(scan_server, scan_port, "/lsworkers", {"classname": monitor_queue}))
			procs = json.load(simplepost.post(scan_server, scan_port, "/lsprocs", {"classname": monitor_queue}))
			break
		except Exception as e:
			print ts(), "Temporary communication failure?", e
			time.sleep(5)

	waiting_tasks = len(filter(lambda x: x["worker"] is None, procs.itervalues()))

	return float(len(filter(lambda x : x["busy"], workers.itervalues())) + waiting_tasks) / len(workers)

while True:

	finished = []
	for proc, cmd in running_procs:
		if proc.poll() is not None:
			if proc.returncode != 0:
				print ts(), "Warning! Process", list(cmd), "exited with code", proc.returncode
			else:
				print ts(), "Process", cmd, "completed"

	high_target = random.uniform(0.9, 1.2)
	low_target = random.uniform(0.5, 0.8)

	print ts(), "Raising utilisation to", high_target
	
	while get_utilisation() < high_target:

		jobdir = os.path.join(job_root, str(next_job))
		tempdir = os.path.join(temp_root, str(next_job))
		print ts(), "Utilisation too low, starting job", next_job
		next_job += 1
		os.mkdir(jobdir)
		logfile = os.path.join(jobdir, "queue.log")
		with open(logfile, "w") as f:
			cmd = [start_job_script, jobdir, tempdir]
			p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
			running_procs.add((p, tuple(cmd)))

		time.sleep(5)

	print ts(), "Utilisation rose to", get_utilisation(), "waiting for fall to", low_target

	while get_utilisation() > low_target:

		time.sleep(5)


