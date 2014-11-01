#!/usr/bin/env python

import sys
import datetime
import os
import json

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: generate_deployment_graphs.py queuelogs_directory jclogs_directory schedlog_file cluster_file"
    sys.exit(1)

qdir = sys.argv[1]
jcdir = sys.argv[2]
schedfile = sys.argv[3]
clusterfile = sys.argv[4]

with open(clusterfile, "r") as f:
    cluster = json.load(f)

max_jobs = dict()

for node in cluster:
    name = node["name"]
    if name not in max_jobs:
        max_jobs[name] = 0
    max_jobs[name] += 1

rawdata = dict()

def add_rawdata(series, node, ts, value):

    if series not in rawdata:
        rawdata[series] = dict()
    if node not in rawdata[series]:
        rawdata[series][node] = []
    rawdata[series][node].append((ts, value))

def get_rawdata_series(series, node):
    if series in rawdata and node in rawdata[series]:
        return rawdata[series][node]
    else:
        return None

# rawdata : seriesname -> nodename -> (value, timestamp) list

# Fetch raw series.
# First: Queue running times.

qevents = dict()
job_start_times = dict()
job_stop_times = dict()

# qevents: node -> (delta, timestamp) list

for rundir in os.listdir(qdir):

    with open(os.path.join(qdir, rundir, "queue.log"), "r") as f:
        loglines = f.readlines()

    startdate = None
    runnode = None
    days = 0

    firsttime = None
    lasttime = datetime.time(0, 0, 0, 0)

    for l in loglines:
        
        if runnode is None:
            off = l.find("Executing as")
            if off != -1:
                l = l[off:]
                usernode = l.split()[2]
                runnode = usernode.split("@")[1]

        if startdate is None:
            off = l.find("Date/Time:")
            if off != -1:
                startdate = datetime.datetime.strptime(l[off:].split()[1], "%Y/%m/%d").date()

        bits = l.split()
        if bits[0] != "INFO":
            continue
        timestr = bits[1].split(",")[0]
        linetime = datetime.datetime.strptime(timestr, "%H:%M:%S").time()

        if firsttime is None:
            firsttime = linetime

        if linetime < lasttime:
            # Rolled over midnight
            days += 1
        
        lasttime = linetime
        if startdate is not None:
            linedt = datetime.datetime.combine(startdate + datetime.timedelta(days = days), linetime)

        if l.find("Submitted job id:") != -1:
            assert linedt is not None
            pid = int(l.split()[-1])
            job_start_times[pid] = linedt

        elif l.find("Job id") != -1 and l.find("done") != -1:
            assert linedt is not None
            pid = int(l.split()[-2])
            job_stop_times[pid] = linedt

    if startdate is None:
        print >>sys.stderr, "Warning: ignore queue file", rundir
        continue

    startts = datetime.datetime.combine(startdate, firsttime)
    endts = datetime.datetime.combine(startdate + datetime.timedelta(days = days), lasttime)

    if runnode not in qevents:
        qevents[runnode] = []
    qevents[runnode].append((startts, 1))
    qevents[runnode].append((endts, -1))

# Convert these delta events into running totals.

for node, deltas in qevents.iteritems():

    running_total = 0
    deltas = sorted(deltas, key = lambda x: x[0])
    for ts, delta in deltas:
        running_total += delta
        assert running_total >= 0
        add_rawdata(series = "coord_jobs", node = node, ts = ts, value = running_total)

qevents = None

# Next read the scheduler log to figure out how many jobs were active on each node:

def add_jobs_delta(node, ts, delta):

    series = get_rawdata_series(node = node, series = "gatk_jobs")
    if series is None:
        last = 0
    else:
        last = series[-1][1]

    add_rawdata(series = "gatk_jobs", node = node, ts = ts, value = last + delta)

job_events = []

with open(schedfile, "r") as f:

    last_time = None

    for l in f:

        if l.startswith("127.0.0.1"):
            bits = l.split()
            last_time = datetime.datetime.strptime(bits[3], "[%d/%b/%Y:%H:%M:%S]")

        elif l.startswith("Start process"):

            bits = l.split()
            pid = int(bits[2])
            if pid not in job_start_times:
                # Job from before this experiment began
                print >>sys.stderr, "Warning: ignore job", pid
                continue

            usernode = bits[6]
            node = usernode.split("@")[1][:-2]
        
            job_events.append((node, job_start_times[pid], 1))
            if pid in job_stop_times: # Ongoing?
                job_events.append((node, job_stop_times[pid], -1))

job_events = sorted(job_events, key = lambda x: x[1])
for ev in job_events:

    add_jobs_delta(*ev)

# Job accounting this way can be perturbed by lag between the scheduler starting a job and Queue noticing that the old one has finished. Limit to the maximum number of jobs permissible on a given node:

for node, points in rawdata["gatk_jobs"].iteritems():

    lim = max_jobs[node]
    for i in range(len(points)):
        points[i] = (points[i][0], min(lim, points[i][1]))

for series, nodes in rawdata.iteritems():

    print series
    for node, points in nodes.iteritems():

        print "\t%s" % node
        for (ts, val) in points:
            
            print "\t\t%s" % ts.isoformat(), val
