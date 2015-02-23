#!/usr/bin/env python

import params
import sys

# For each cost tier, report whether scaling each pipeline stage is profitable, and if so to what level.

job_scaling_factor = 1.0

reward_fn = params.time_reward

for arg in sys.argv:
    if arg.startswith("jobscaling="):
        job_scaling_factor = float(arg[len("jobscaling="):])
    elif arg.startswith("thread_factors="):
        tier_bits = arg[len("thread_factors="):].split(",")
        params.thread_factor_params = [float(x) for x in tier_bits]
    elif arg == "rewardfn=throughput":
        reward_fn = params.throughput_reward

params.reward = reward_fn
    
for i in range(len(params.thread_factor_params)):
    params.thread_factor_params[i] *= job_scaling_factor
    if params.thread_factor_params[i] > 1:
        params.thread_factor_params[i] = 1

job_size = 5
print "Assumed job size:", job_size

def plan_times(plan):
    return [params.processing_time(job_size, cores, 1, phase, False) for (phase, cores) in enumerate(plan)]

def plan_cost(plan, times, costpercore):
    return costpercore * sum([x * y for (x, y) in zip(plan, times)])

for (i, tier) in enumerate(params.core_cost_tiers):

    print "Tier", i+1

    baseline_times = plan_times([1] * 7)
    baseline_cost = plan_cost([1] * 7, baseline_times, tier["cost"])
    print "Expected cost of single-threaded run:", baseline_cost
    baseline_profit = params.reward(sum(baseline_times), job_size) - baseline_cost
    print "Base profit:", baseline_profit

    for stage in range(7):

        best_profit = baseline_profit
        best_cores = 1

        for cores in params.dynamic_core_choices[1:]:

            plan = [1] * 7
            plan[stage] = cores
            times = plan_times(plan)
            cost = plan_cost(plan, times, tier["cost"])
            profit = params.reward(sum(times), job_size) - cost

            if profit > best_profit:
                best_profit = profit
                best_cores = cores

        print "Stage", stage+1, "runs best with", best_cores, "cores (best profit = %g)" % best_profit

    print


    

    
