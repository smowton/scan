#!/usr/bin/python

import sys
import matplotlib.pyplot as plt

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

for i, (core_points, job_points, times) in enumerate(zip(cores_points, jobs_points, series_times)):

    arg = int("%s%s%s" % (len(cores_points), 1, i+1))
    plt.subplot(arg)
    plt.plot(times, core_points)
    plt.plot(times, job_points)
    lims = plt.ylim()
    plt.ylim(lims[0], lims[1] + 1)

plt.show()
