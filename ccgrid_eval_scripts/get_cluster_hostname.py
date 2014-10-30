#!/usr/bin/env python

import sys
import json
import os.path

with open(sys.argv[1], "r") as f:
	cluster = json.load(f)
	jobidx = int(os.path.split(sys.argv[2])[1])
	sys.stdout.write(cluster[jobidx % len(cluster)]["ip"])

