#!/usr/bin/python

import sys

import ccgrid_graphing.stackplot

if len(sys.argv) < 2:
    save_file = None
else:
    save_file = sys.argv[1]

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

# Draw a plotlet for each phase, and within that, a series for cores and a series for jobs.

plots = []

for (core_points, job_points, times) in zip(cores_points, jobs_points, series_times):

    plots.append([(times, job_points), (times, core_points)])

ccgrid_graphing.stackplot.draw_stackplot(plots, xlabel = "Sim time elapsed (minutes)", ylabel = "Active jobs or cores", save_file = save_file)

