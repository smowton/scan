#!/usr/bin/env python

import sys
import datetime
import os
import json

import ccgrid_graphing.stackplot

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: generate_deployment_graphs.py queuelogs_directory jclogs_directory schedlog_file cluster_file outdir"
    sys.exit(1)

qdir = sys.argv[1]
jcdir = sys.argv[2]
schedfile = sys.argv[3]
clusterfile = sys.argv[4]
outdir = sys.argv[5]

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

    qfile = os.path.join(qdir, rundir, "queue.log")
    print >>sys.stderr, "Read", qfile

    with open(qfile, "r") as f:
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

print >>sys.stderr, "Read", schedfile
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
                # print >>sys.stderr, "Warning: ignore job", pid
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

# Finally, acquire data series from the workers' JCatascopia logs.

for nodelog in os.listdir(jcdir):

    node = nodelog
    if node.endswith(".log"):
        node = node[:-4]
        
    lfile = os.path.join(jcdir, nodelog)
    print >>sys.stderr, "Read", lfile
    with open(lfile, "r") as f:

        for l in f:

            if not l.startswith("{"):
                continue

            rec = json.loads(l.strip())
            if "timestamp" not in rec:
                continue

            ts = datetime.datetime.fromtimestamp(long(rec["timestamp"]) / 1000)

            for met in rec["metrics"]:
                series = "%s_%s" % (rec["group"], met["name"])
                if met["type"] == "STRING":
                    val = met["val"]
                else:
                    val = float(met["val"])
                    
                add_rawdata(series = series, node = node, ts = ts, value = val)
                
# Create derived series:

def sum_series(in1, in2, out):

    assert out not in rawdata
    rawdata[out] = dict()

    datapairs = [(x, rawdata[in1][x], rawdata[in2][x]) for x in rawdata[in1]]

    for (node, in1points, in2points) in datapairs:
        rawdata[out][node] = [(ts, x + y) for ((ts, x), (ts2, y)) in zip(in1points, in2points)]

def derive_series(ins, outs, f):

    assert outs not in rawdata
    rawdata[outs] = dict()

    for (node, points) in rawdata[ins].iteritems():
        
        rawdata[outs][node] = [(ts, f(x)) for (ts, x) in points]

print >>sys.stderr, "Create derived series"
sum_series("NetworkProbe_netBytesIN", "NetworkProbe_netBytesOUT", "NetworkProbe_netBytesTotal")
derive_series("NetworkProbe_netBytesTotal", "NetworkProbe_netkbps", lambda x: x / 1024)
sum_series("DiskStatsProbe_readkbps", "DiskStatsProbe_writekbps", "DiskStatsProbe_totalkbps")

print >>sys.stderr, "Calculate aggregate series"

draw_series = ["coord_jobs", "gatk_jobs", "CPUProbe_cpuTotal", "MemoryProbe_memUsed", "NetworkProbe_netkbps", "DiskStatsProbe_totalkbps"]

aggseries = dict()

# Force all series to start with the first coordinator point
first_ts = None
latest_ts = None

def totalseconds(td):

    return float(td.seconds + (td.days * 24 * 3600))

for series in draw_series:

    alldata = []
    for node, points in rawdata[series].iteritems():
        alldata.extend([(node, ts, val) for (ts, val) in points])

    alldata = sorted(alldata, key = lambda x : x[1])

    aggdata = []
    latestvals = dict()

    if first_ts is None:
        first_ts = alldata[0][1]
    
    for (node, ts, val) in alldata:

        latestvals[node] = val
        if ts < first_ts:
            continue

        aggdata.append((ts - first_ts, sum(latestvals.values())))

    if latest_ts is None or alldata[-1][1] > latest_ts:
        latest_ts = alldata[-1][1]

    aggseries[series] = aggdata

# Extend short series with a flat line to the end.

for aggdata in aggseries.itervalues():

    last_td, last_val = aggdata[-1]

    if last_td < (latest_ts - first_ts):
        aggdata.append((latest_ts - first_ts, last_val))

# Express times as hours since the start, and tabulate for matplotlib:

graphseries = dict()

for series, aggdata in aggseries.iteritems():
    graphseries[series] = ([totalseconds(x) / (60 * 60) for (x, y) in aggdata], [y for (x, y) in aggdata])

# All data acquired. Draw the big summary graph.

ccgrid_graphing.stackplot.draw_stackplot([[x] for x in graphseries.itervalues()], xlabel = "Timestamp", ylabel = "Value", save_file = os.path.join(outdir, "summary.pdf"))

