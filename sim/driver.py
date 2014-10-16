#!/usr/bin/python

import sim
import sys

if "dyn" in sys.argv:
    nmachines = [None] * 7
    if len(sys.argv) > 2:
        print >>sys.stderr, "dyn and other options are mutually exclusive"
        sys.exit(1)

nmachines = None
machine_specs = [1]
debug = False

for arg in sys.argv[1:]:
    if arg.startswith("nmachines="):
        nmachines = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("machine_specs="):
        machine_specs = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg == "debug":
        debug = True

if nmachines is None:
    nmachines = [None] * len(machine_specs)

if len(nmachines) != len(machine_specs):
    print >>sys.stderr, "Given nmachines and machine_specs must have matching length"
    sys.exit(1)

if len(nmachines) != 1 and len(nmachines) != 7:
    print >>sys.stderr, "Must specify either 1 or 7 machine classes"
    sys.exit(1)

arrival_process = sim.ArrivalProcess(mean_arrival = 3000, mean_jobs = 3, jobs_var = 2, mean_records = 1000, records_var = 200)
state = sim.SimState(nmachines = nmachines, machine_specs = machine_specs, arrival_process = arrival_process, stop_time = 1000000, debug = debug)

state.run()

print >>sys.stderr, "Total reward", state.total_reward
print >>sys.stderr, "Total cost", state.total_cost
print >>sys.stderr, "Reward per unit cost", float(state.total_reward) / state.total_cost
