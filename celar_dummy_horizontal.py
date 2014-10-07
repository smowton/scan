#!/usr/bin/python

import json
import sys
import simplepost
import subprocess
import time

# Dummy CELAR replacement for testing SCAN. It should regularly read the properties of the scheduler running on this host
# and mimic the actions of the decision module and orchestrator combined by implementing policy and making scaling decisions.
# For now we assume that a bunch of workers (which can mention the same hostname multiple times if desired) are given on the command line,
# and that all such workers are suitable for running all classes of task for which demand arises.

if len(sys.argv) < 2:
    print >>sys.stderr, "Usage: clear_dummy.py worker_list"
    sys.exit(1)

free_workers = []

def json_get(address, params = {}):
    return json.loads(simplepost.post("localhost", 8080, address, params).read())

with open(sys.argv[1], "r") as f:
    
    for line in f:
        line = line.strip()
        if len(line) == 0:
            continue
        free_workers.append(line)

classes = json_get("/getclasses")

while True:

    made_change = False

    for c in classes:

        queuelen = len([x for x in json_get("/lsprocs", {"classname": c}).itervalues() if x["worker"] is None])
        workers = json_get("/lsworkers", {"classname": c})

        if len(workers) == 0:
            utilisation = 1.0
        else:
            utilisation = float(len([w for w in workers.itervalues() if w["busy"]])) / len(workers)

        if queuelen > 0:
            if len(free_workers) != 0:

                print "Class", c, "has pending work: add worker"
                wid = json_get("/addworker", {"address": free_workers[-1], "classname": c})["wid"]
                print "Added worker %s (wid %d)" % (free_workers[-1], wid)
                free_workers.pop()
                made_change = True

            else:
                print "Class", c, "needs more workers, but none are free"
        
        elif utilisation < 0.8:

            print "Class", c, "has too many idle workers (utilisation: %g)" % utilisation
            ret = subprocess.check_output(["./blocking_delete.py", c])
            bits = ret.split(":")
            if len(bits) < 3:
                print >>sys.stderr, "Blocking delete failed?"
                continue
            print "Reclaimed worker %s (wid %s)" % (bits[1], bits[2].strip())
            free_workers.append(bits[1])
            made_change = True

    if not made_change:
        time.sleep(10)
