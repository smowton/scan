#!/usr/bin/python

import sys
import json
import subprocess
import datetime

if len(sys.argv) < 2:
    print '{"error": "Usage: getres.py pidfile"}'
    sys.exit(0)

def getres(pidfile):

    with open(pidfile, "r") as f:
        pid = int(f.read().strip())

    # Get process group:
    cmd = ["/bin/ps", "--no-header", "-p", str(pid), "-o", "pgrp"]
    try:
        pgrp = int(subprocess.check_output(cmd).strip())
    except Exception as e:
        raise Exception("No such process %d (cause: %s)" % (pid, e))

    psproc = subprocess.Popen(["/bin/ps", "ax", "-o", "pgrp,pid,cputime,rss", "--no-header"], stdout=subprocess.PIPE)
    
    totalcpu = 0
    totalrss = 0

    for l in psproc.stdout:
        bits = [x.strip() for x in l.split()]
        if len(bits) != 4:
            continue
        if int(bits[0]) == pgrp:
            
            cpubits = bits[2].split(":")
            totalcpu += int(cpubits[2])
            totalcpu += (int(cpubits[1]) * 60)
            totalcpu += (int(cpubits[0]) * 60 * 60)
            
            totalrss += int(bits[3])

    json.dump({"cpuseconds": totalcpu, "rsskb": totalrss}, sys.stdout)

try:
    getres(sys.argv[1])
except Exception as e:
    print json.dumps({"error": str(e)})

