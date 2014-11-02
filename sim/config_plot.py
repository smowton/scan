#!/usr/bin/env python

# Find the best point in some dataset. Plot the changes in performance obtained by varying one of nmachines, machine_specs, and phase_splits.

import sys
import os.path
import bestresult
import explore_bestresult
import hillclimb
import matplotlib.pyplot as plt

def plot_errbars(points, xlabel, filename):

    points = sorted(points, key = lambda x: x[0])
    
    xs = [x for (x, ys) in points]
    ys = [ymed for (x, (ymin, ymed, ymax)) in points]
    lowerror = [ymed - ymin for (x, (ymin, ymed, ymax)) in points]
    higherror = [ymax - ymed for (x, (ymin, ymed, ymax)) in points]
    plt.figure(figsize=(4,4))
    plt.errorbar(xs, ys, yerr=(lowerror, higherror), color='k')
    plt.xlabel(xlabel)
    plt.ylabel("Reward-to-cost ratio")
    plt.tight_layout()
    print "Writing", filename
    plt.savefig(filename)

def plot_aggregate(params, axis, xlabel, filename):

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

    plot_errbars(points, xlabel, filename)

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: config_plot.py data_dir out_dir plot_title"

data_dir = sys.argv[1]
out_dir = sys.argv[2]

hillclimb.workdir = data_dir

out_machines = os.path.join(out_dir, "nmachines.pdf")
out_specs = os.path.join(out_dir, "specs.pdf")
out_splits = os.path.join(out_dir, "splits.pdf")

best_params = bestresult.get_best_result_in(data_dir)[0]["spec"]

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
    plot_aggregate(machine_vars, "nmachines", "Total number of machines employed", out_machines)
if spec_vars is not None:
    plot_aggregate(spec_vars, "machine_specs", "Total core-stages per pipeline run", out_specs)
if split_vars is not None:
    plot_aggregate(split_vars, "phase_splits", "Total processes per pipeline run", out_splits)
