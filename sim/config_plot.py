#!/usr/bin/env python

# Find the best point in some dataset. Plot the changes in performance obtained by varying one of nmachines, machine_specs, and phase_splits.

import sys
import os.path
import bestresult
import explore_bestresult
import hillclimb
import matplotlib.pyplot as plt

def plot_errbars(points, filename):

    xs = [x for (x, ys) in points]
    ys = [ymed for (x, (ymin, ymed, ymax)) in points]
    errorbars = [(ymed - ymin, ymax - ymed) for (x, (ymin, ymed, ymax)) in points]
    plt.figure()
    plt.plot(xs, ys, yerr=errorbars)
    plt.savefig(filename)

def plot_aggregate(params, axis, filename):

    agg_ratios = dict()

    for param in params:

        try:
            ratios = [hillclimb.read_try_result({"try": i, "params": param})["ratio"] for i in range(10)]
            avg_ratio = sum(ratios) / len(ratios)
            agg_key = sum(param[axis])
            if agg_key not in agg_ratios:
                agg_ratios[agg_key] = []
            agg_ratios[agg_key].append(avg_ratio)
        except Exception as e:
            print >>sys.stderr, "Warning, ignoring malformed params", param, "(%s)" % e

    agg_ratios = [(k, sorted(v)) for k, v in agg_ratios.iteritems()]

    points = [(k, (v[0], v[len(v)/2], v[-1])) for (k, v) in agg_ratios]

    plot_errbars(points, filename)

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: config_plot.py data_dir out_dir"

data_dir = sys.argv[1]
out_dir = sys.argv[2]

out_machines = os.path.join(out_dir, "nmachines.pdf")
out_specs = os.path.join(out_dir, "specs.pdf")
out_splits = os.path.join(out_dir, "splits.pdf")

best_params = get_best_result_in(data_dir)

print "Plotting based on best point", best_params

machine_vars, spec_vars, split_vars = None, None, None

if best_params["nmachines"][0] is not None:
    machine_vars = explore_bestresult.collect_machine_vars(best_params)
if best_params["machine_specs"][0] is not None:
    spec_vars = explore_bestresult.collect_spec_vars(best_params)
if best_params["phase_splits"][0] is not None:
    split_vars = explore_bestresult.collect_split_vars(best_params)

def validate(l):
    if l is None:
        return None
    else:
        return filter(hillclimb.valid_params, l)

(machine_vars, spec_vars, split_vars) = map(validate, (machine_vars, spec_vars, split_vars))

if machine_vars is not None:
    plot_aggregate(machine_vars, "nmachines", "nmachines.pdf")
if spec_vars is not None:
    plot_aggregate(spec_vars, "machine_specs", "machine_specs.pdf")
if split_vars is not None:
    plot_aggregate(split_vars, "phase_splits", "phase_splits.pdf")
