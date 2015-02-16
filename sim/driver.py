#!/usr/bin/python

import sim
import sys
import json
import params

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
hscale_algorithm = "predict"
hscale_params = {}
vscale_algorithm = "greedy"
vscale_params = {"greed": "1", "queuefactor": "1"}
job_scaling_factor = 1.0
arrival_proc = "normal"
arrival_proc_args = {"mean_arrival": "3.0"}

for arg in sys.argv[1:]:
    if arg.startswith("nmachines="):
        nmachines = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("ncores="):
        ncores = int(arg.split("=")[1])
    elif arg.startswith("machine_specs="):
        machine_specs = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("phase_splits="):
        phase_splits = [int(x) for x in arg.split("=")[1].split(",")]
    elif arg.startswith("vscale="):
        vscale_bits = arg[len("vscale="):].split(",")
        vscale_algorithm = vscale_bits[0]
        vscale_params = dict([x.split("=") for x in vscale_bits[1:]])
    elif arg.startswith("jobscaling="):
        job_scaling_factor = float(arg[len("jobscaling="):])
    elif arg.startswith("arrivalproc="):
        arrival_bits = arg[len("arrivalproc="):].split(",")
        arrival_proc = arrival_bits[0]
        arrival_proc_args = dict([x.split("=") for x in arrival_bits[1:]])
    elif arg.startswith("startupdelay="):
        params.vm_startup_delay = float(arg.split("=")[1])
    elif arg.startswith("predictionerror="):
        params.runtime_prediction_error = float(arg.split("=")[1])
    elif arg.startswith("hscale="):
        hscale_bits = arg[len("hscale="):].split(",")
        hscale_algorithm = hscale_bits[0]
        hscale_params = dict([x.split("=") for x in hscale_bits[1:]])        
    elif arg.startswith("tier_costs="):
        tier_bits = arg[len("tier_costs="):].split(",")
        def parse_cores_str(cores):
            if cores == "*":
                return None
            else:
                return int(cores)
        split_tiers = [bit.split(":") for bit in tier_bits]
        params.core_cost_tiers = [{"cores": parse_cores_str(cores), "cost": int(cost)} for (cores, cost) in split_tiers]
    elif arg.startswith("thread_factors="):
        tier_bits = arg[len("thread_factors="):].split(",")
        params.thread_factor_params = [float(x) for x in tier_bits]            
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

for i in range(len(params.thread_factor_params)):
    params.thread_factor_params[i] *= job_scaling_factor
    if params.thread_factor_params[i] > 1:
        params.thread_factor_params[i] = 1

if arrival_proc == "normal":
    arrival_process = sim.NormalArrivalProcess(**arrival_proc_args)
elif arrival_proc == "weekend":
    arrival_process = sim.WeekendArrivalProcess(**arrival_proc_args)
state = sim.SimState(nmachines = nmachines, ncores = ncores, machine_specs = machine_specs, phase_splits = phase_splits, arrival_process = arrival_process, stop_time = 10000, debug = debug, plot = plot, hscale_algorithm = hscale_algorithm, hscale_params = hscale_params, vscale_algorithm = vscale_algorithm, vscale_params = vscale_params)

state.run()

avg_profit = (state.total_reward - state.total_cost) / len(state.completed_jobs)

avg_queue_time = sum([j.total_queue_delay for j in state.completed_jobs])
avg_queue_time /= len(state.completed_jobs)

total_run_time = sum([(j.actual_finish_time - j.start_time) for j in state.completed_jobs])
avg_run_time = total_run_time / len(state.completed_jobs)

queue_pc = ((avg_queue_time * 100) / avg_run_time)

configs = dict()

for job in state.completed_jobs:
    config = job.stage_configs
    key = tuple([v["cores"] for v in config])
    if key not in configs:
        configs[key] = []
    configs[key].append(job)

top_configs = []

for config, jobs in sorted(configs.iteritems(), key = lambda k : len(k[1]), reverse = True)[:10]:

    this_config_reward = sum([job.reward for job in jobs])
    this_config_cost = sum([job.cost for job in jobs])
    top_configs.append({"config": config, "njobs": len(jobs), "avgprofit": (this_config_reward - this_config_cost) / len(jobs)})

if print_json:

    print json.dumps({"reward": state.total_reward, "cost": state.total_cost, "avgprofit": avg_profit, "avgqueuetime": avg_queue_time, "avgruntime": avg_run_time, "queuepc": queue_pc, "top_configs": top_configs})

else:

    print >>sys.stderr, "Total reward", state.total_reward
    print >>sys.stderr, "Total cost", state.total_cost
    print >>sys.stderr, "Average profit", avg_profit

    for tier, (tus, costpertu) in enumerate(zip(state.cost_by_tier, [tier["cost"] for tier in params.core_cost_tiers])):
        print >>sys.stderr, "Cost at tier %d: %d" % (tier + 1, tus * costpertu)

    print >>sys.stderr, "Average job spent", avg_queue_time, "waiting in queues, %g%% of total runtime" % queue_pc

    print >>sys.stderr, "Most common configs used (total jobs = %d):" % len(state.completed_jobs)
    for config in top_configs:

        print >>sys.stderr, "%s: %d (average profit %g)" % (config["config"], config["njobs"], config["avgprofit"])

