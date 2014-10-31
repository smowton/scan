#!/usr/bin/python

import sys
import matplotlib
import matplotlib.pyplot as plt

if len(sys.argv) < 2:
    save_file = None
else:
    save_file = sys.argv[1]

matplotlib.rcParams['font.size'] = 8

jobs_colour = 'k'
cores_colour = '0.25'

jobs_marker = ' '
cores_marker = '|'

cores_points = []
jobs_points = []
series_times = []

for line in sys.stdin:

    line = line.strip()
    if line == "":
        continue

    time, series, cores, jobs = line.split(",")
    time = float(time)
    series = int(series)
    cores = int(cores)
    jobs = int(jobs)

    while len(cores_points) <= series:
        cores_points.append([])
        jobs_points.append([])
        series_times.append([])

    cores_points[series].append(cores)
    jobs_points[series].append(jobs)
    series_times[series].append(time)

def downsample(series, order):

    newseries = []
    i = 0
    while i < len(series):
        grp = series[i:i+order]
        newseries.append(float(sum(grp)) / len(grp))
        i += order
    return newseries

def downsample_list(l, order):
    return [downsample(v, order) for v in l]

if len(cores_points[0]) > 100:

    order = len(cores_points[0]) / 100
    print "Downsampling using groups of", order, "points"
    cores_points = downsample_list(cores_points, order)
    jobs_points = downsample_list(jobs_points, order)
    series_times = downsample_list(series_times, order)

fig = plt.figure(figsize = (4, len(cores_points) * 2), dpi=300)
masterax = fig.add_subplot(111, axisbg='none')
masterax.spines['top'].set_color('none')
masterax.spines['bottom'].set_color('none')
masterax.spines['left'].set_color('none')
masterax.spines['right'].set_color('none')
masterax.tick_params(labelcolor='none', top='off', bottom='off', left='off', right='off')
   
for i, (core_points, job_points, times) in enumerate(zip(cores_points, jobs_points, series_times)):

    arg = int("%s%s%s" % (len(cores_points), 1, i+1))
    ax = fig.add_subplot(arg)
    ax.plot(times, core_points, color=cores_colour, marker=cores_marker)
    ax.plot(times, job_points, color=jobs_colour, marker=jobs_marker)
    if i != len(cores_points) - 1:
        ax.set_xticklabels([])
    else:
        ax.set_xlabel("Sim time elapsed (minutes)")
    lims = ax.get_ylim()
    ax.set_ylim(lims[0], lims[1] + 1)

masterax.set_ylabel("Active jobs or cores")

if save_file is None:
    plt.show()
else:
    plt.savefig(save_file)
