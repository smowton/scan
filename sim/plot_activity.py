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
    
    bits = line.split(",")
    if len(bits) == 4:
        time, series, cores, jobs = bits
    elif len(bits) == 3:
        time, series, jobs = bits
        cores = None
    time = float(time)
    series = int(series)
    if cores is not None:
        cores = int(cores)
    jobs = int(jobs)

    while len(cores_points) <= series:
        cores_points.append([])
        jobs_points.append([])
        series_times.append([])

    if cores is not None:
        cores_points[series].append(cores)
    jobs_points[series].append(jobs)
    series_times[series].append(time)

def downsample(series, order):

    if len(series) == 0:
        return series

    newseries = []
    i = 0
    while i < len(series):
        grp = series[i:i+order]
        newseries.append(float(sum(grp)) / len(grp))
        i += order
    return newseries

def downsample_list(l, order):
    return [downsample(v, order) for v in l]

if len(cores_points[0]) > 1000:

    order = len(cores_points[0]) / 500
    print "Downsampling using groups of", order, "points"
    cores_points = downsample_list(cores_points, order)
    jobs_points = downsample_list(jobs_points, order)
    series_times = downsample_list(series_times, order)

# Draw a plotlet for each phase, and within that, a series for cores and a series for jobs.

plots = []

if len(jobs_points) == 1:
    phase_titles = ["Active analysis stages or cores over simulation period"]
elif len(jobs_points) == 3:
    phase_titles = ["Active analysis tasks",
                    "Queued transfers public -> private",
                    "Queued transfers private -> public"]
else:
    phase_titles = ["Active phase 1 (RealignerTargetCreator) tasks",
                    "Active phase 2 (IndelRealigner) tasks",
                    "Active phase 3 (BQSR) tasks",
                    "Active phase 4 (PrintReads) tasks",
                    "Active phase 5 (UnifiedGenotyper) tasks",
                    "Active phase 6 (VariantFiltration) tasks",
                    "Active phase 7 (VariantEval) tasks"]
    if len(jobs_points) == 8:
        phase_titles.append("Active gather tasks")

for (core_points, job_points, times, title) in zip(cores_points, jobs_points, series_times, phase_titles):

    series = [(times, job_points)]
    if len(core_points) > 0:
        series.append((times, core_points))
    plots.append((title, series))

ccgrid_graphing.stackplot.draw_stackplot(plots, xlabel = "Sim time elapsed (TUs)", ylabel = "Active stages or cores", save_file = save_file)

