#!/usr/bin/env python

import sys
import os.path
import bestresult
import hillclimb
import subprocess

if len(sys.argv) < 3:
    print >>sys.stderr, "Usage: config_plot.py data_dir out_dir plot_title"

data_dir = sys.argv[1]
out_dir = sys.argv[2]

hillclimb.workdir = data_dir

out_file = os.path.join(out_dir, "trace.pdf")

best_params = bestresult.get_best_result_in(data_dir)[0]["spec"]

print "Plotting based on best point", best_params

cmdline = ["/usr/bin/python", "-m", "sim.driver"]

if best_params["nmachines"][0] is not None:
    cmdline.append("nmachines=" + ",".join([str(n) for n in best_params["nmachines"]]))
if best_params["machine_specs"][0] is not None:
    cmdline.append("machine_specs=" + ",".join([str(n) for n in best_params["machine_specs"]]))
if best_params["phase_splits"][0] is not None:
    cmdline.append("phase_splits=" + ",".join([str(n) for n in best_params["phase_splits"]]))

print "Run", cmdline

sim = subprocess.Popen(cmdline, stdout=subprocess.PIPE)
subprocess.check_call(["/usr/bin/python", "-m", "sim.plot_activity", out_file], stdin = sim.stdout)

