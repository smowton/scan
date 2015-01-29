#!/usr/bin/python

import sim
import sys
import json

if "dyn" in sys.argv:
    nmachines = [None] * 7
    if len(sys.argv) > 2:
        print >>sys.stderr, "dyn and other options are mutually exclusive"
        sys.exit(1)

ncores = None
nmachines = None
machine_specs = [None]
phase_splits = None
debug = False
print_json = False
plot = True
greed = 1.5

for arg in sys.argv[1:]:
    if arg.startswith("nmachines="):
        nmachines = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("ncores="):
        ncores = int(arg.split("=")[1])
    elif arg.startswith("machine_specs="):
        machine_specs = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("phase_splits="):
        phase_splits = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("greed="):
        greed = float(arg[len("greed="):])
    elif arg == "debug":
        debug = True
    elif arg == "noplot":
        plot = False
    elif arg == "json":
        print_json = True
    else:
        raise Exception("Unrecognised argument: %s" % arg)

if phase_splits is None:
    phase_splits = [1] * 7

if len(phase_splits) != 7:
    print >>sys.stderr, "Must specify exactly 7 phase splits"
    sys.exit(1)

if ncores is not None:
    if len(machine_specs) != 7 or nmachines is not None:
        print >>sys.stderr, "ncores only currently works with machine_specs specified and nmachines not set."
        sys.exit(1)

if(any([x > 1 for x in phase_splits])):
    multiqueue_count = 8
else:
    multiqueue_count = 7

if nmachines is None:
    nmachines = [None] * len(machine_specs)

if len(nmachines) != len(machine_specs):
    print >>sys.stderr, "Given nmachines and machine_specs must have matching length"
    sys.exit(1)

if len(nmachines) != 1 and len(nmachines) != multiqueue_count:
    print >>sys.stderr, "Must specify either 1 or", multiqueue_count, " [or", "7" if multiqueue_count == 8 else "8", " depending on whether work splitting is in use] machine classes"
    sys.exit(1)

arrival_process = sim.ArrivalProcess(mean_arrival = 3, mean_jobs = 3, jobs_var = 2, mean_records = 5, records_var = 1)
state = sim.SimState(nmachines = nmachines, ncores = ncores, machine_specs = machine_specs, phase_splits = phase_splits, arrival_process = arrival_process, stop_time = 10000, debug = debug, plot = plot, greed = greed)

state.run()

if print_json:

    print json.dumps({"reward": state.total_reward, "cost": state.total_cost, "ratio": float(state.total_reward) / state.total_cost})

else:

    print >>sys.stderr, "Total reward", state.total_reward
    print >>sys.stderr, "Total cost", state.total_cost
    print >>sys.stderr, "Reward per unit cost", float(state.total_reward) / state.total_cost

    for tier, cost in enumerate(state.cost_by_tier):
        print >>sys.stderr, "Cost at tier %d: %d" % (tier + 1, cost)

    configs = dict()

    for config, reward in state.completed_job_configs:
        key = tuple([v["cores"] for v in config])
        if key not in configs:
            configs[key] = []
        configs[key].append(reward)

    print >>sys.stderr, "Most common configs used (total jobs = %d):" % len(state.completed_job_configs)
    for config, rewards in sorted(configs.iteritems(), key = lambda k : len(k[1]), reverse = True)[:10]:

        print "%s: %d (average reward %g)" % (config, len(rewards), float(sum(rewards)) / len(rewards))

