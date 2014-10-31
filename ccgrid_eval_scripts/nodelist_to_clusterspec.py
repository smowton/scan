#!/usr/bin/env python

import os
import sys
import json
import socket

if "PBS_NODEFILE" not in os.environ or "PBS_NUM_NODES" not in os.environ:
    print >>sys.stderr, "No nodefile found; make sure you run this from within a PBS multinode job"
    sys.exit(1)

# The node file will list each node more than once if we launched with the ppn option.

with open(os.environ["PBS_NODEFILE"], "r") as f:
    nodes = f.readlines()

nodes = [n.strip() for n in nodes]
nodes = filter(lambda x: len(x) > 0, nodes)

num_nodes = int(os.environ["PBS_NUM_NODES"])

ppn = len(nodes) / num_nodes

print >>sys.stderr, "Got", num_nodes, "nodes; nodefile has", len(nodes), "entries, so PPN is", ppn

nodect = dict()

for node in nodes:
    if node not in nodect:
        nodect[node] = 0
    nodect[node] += 1

ret_nodes = []

for node, count in nodect.iteritems():
    
    if count % ppn != 0:
        print >>sys.stderr, node, "appears in node list", count, "times, which is not a multiple of PPN", ppn
        sys.exit(1)

    for i in range(count / ppn):

        ret_nodes.append({
            "passwd": "not_required",
            "cores": ppn,
            "name": node,
            "ip": socket.gethostbyname(node),
            "id": node
        })

json.dump(ret_nodes, sys.stdout)

