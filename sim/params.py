
# The customisable, semi-fixed aspects of the simulation

import math
import random

# Processing times: how many time units will it take to process the given number of records?
# When threading or splitting are used, the record count still represents the entire job.
# There are basically two classes of task: the largely input-dependent, and the largely input-independent.
# 1000 records is used to represent input of similar size to the genome.

def correct_thread_efficiency(thread_count):

    # Complete guess, to be replaced by profiled data:
    return float(thread_count) * (1 - (float(thread_count) / 48))

def processing_time(record_count, thread_count, split_count, phase_index, include_gather):

    thread_count = correct_thread_efficiency(thread_count)
    ret = processing_times[phase_index](record_count, thread_count, split_count)
    if include_gather:
        ret += gather_time(record_count, phase_index)
    return ret

task_overhead = 50

def input_independent(record_count, thread_count, split_count):

    return (float(1000) / (thread_count * split_count)) + task_overhead

def input_dependent(record_count, thread_count, split_count):

    return (float(record_count) / (thread_count * split_count)) + task_overhead

def input_dependent_nothreads(record_count, thread_count, split_count):

    return (float(record_count) / (split_count)) + task_overhead

processing_times = [
    input_independent,
    input_dependent_nothreads,
    input_dependent,
    input_dependent,
    input_independent,
    input_dependent_nothreads,
    input_independent
]

can_split_phase = [
    True,
    True,
    True,
    True,
    True,
    True,
    False
]

def gather_time(record_count, phase_index):

    # Say roughly 10% of the time required to actually process a record.
    return float(record_count) / 10

def reward(total_pipeline_latency, record_count):

    # For now let's suppose we don't care about record count:
    # the reward structure is 1000 points for finishing in time, 
    # 500 for being a little late, 1500 for being quite early, 0 otherwise.

    if total_pipeline_latency < 5000:
        return 1500
    elif total_pipeline_latency < 11000:
        score = float(11000 - total_pipeline_latency) / 6000
        return 500 + (score * 1000)
    else:
        return 0

core_cost_tiers = [{"cores": 100, "cost": 0.02}, {"cores": -1, "cost": 0.1}]

def core_tier(cores):

    for i, tier in enumerate(core_cost_tiers):
        if tier["cores"] == -1:
            return i
        cores -= tier["cores"]
        if cores <= 0:
            return i

    raise Exception("Bad core_cost_tiers")

def concurrent_cores_hired_to_cost(cores):

    # Model a simple situation where we own a bunch of cores (which are thus nearly free)
    # but can also hire more from the cloud (more expensive)
    # Benchmark: putting a 1000-record input through the pipeline with a single core takes around 8000 time units
    # and earns 1000 money. Thus hiring a core for 0.1 money / time unit is an OK price.

    return (min(cores, 100) * 0.02) + (max(0, cores - 100) * 0.1)

dynamic_core_choices = [1, 2, 4, 8, 16]
dynamic_core_greed_factor = 1.5

vm_startup_delay = 50

def predicted_to_real_time(predicted_time):

    return predicted_time * random.uniform(0.9, 1.1)
