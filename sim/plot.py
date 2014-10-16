#!/usr/bin/python

import sys
import matplotlib.pyplot as plt

series_points = []
series_times = []

for line in sys.stdin:

    line = line.strip()
    if line == "":
        continue

    time, series, level = line.split(",")
    time = float(time)
    series = int(series)
    level = int(level)

    while len(series_points) <= series:
        series_points.append([])
        series_times.append([])

    series_points[series].append(level)
    series_times[series].append(time)

for i, (points, times) in enumerate(zip(series_points, series_times)):

    arg = int("%s%s%s" % (len(series_points), 1, i+1))
    plt.subplot(arg)
    plt.plot(times, points)
    lims = plt.ylim()
    plt.ylim(lims[0], lims[1] + 1)

plt.show()
