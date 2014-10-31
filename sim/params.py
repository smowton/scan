
# The customisable, semi-fixed aspects of the simulation

import math
import random

# Processing times: how many time units will it take to process the given number of records?
# When threading or splitting are used, the record count still represents the entire job.
# There are basically two classes of task: the largely input-dependent, and the largely input-independent.

def correct_thread_efficiency(thread_count):

    # Complete guess, to be replaced by profiled data:
    return float(thread_count) * (1 - (float(thread_count) / 48))

def processing_time(record_count, thread_count, split_count, phase_index, include_gather):
    
    params = stage_to_time_params[phase_index]
    ret = params[1] + (params[0] * (float(record_count) / split_count))
    if ret < 0:
        ret = 0.0

    ideal_thread_factor = float(1) / thread_count
    thread_constant_part = thread_factor_params[phase_index][1]

    ret *= (thread_constant_part + ((1 - thread_constant_part) * ideal_thread_factor))

    if include_gather:
        ret += gather_time(record_count, phase_index)

    return ret

thread_factor_params = [
    (0.886964886965, 0.111111111111),
    (-0.0106345267636, 0.981389578164),
    (0.687110446273, 0.312390924956),
    (0.780952380952, 0.213333333333),
    (0.905907380134, 0.0904925544101),
    (0.238095238095, 0.75),
    (0.0265780730897, 0.976744186047)
]

stage_to_time_params = [
    (0.3485, 5.3797222222),
    (2.6923333333, -0.5316666667),
    (1.7351666667, 3.9308333333),
    (3.353, 0.5327777778),
    (1.032, 17.86),
    (0.0196666667, 0.3938888889),
    (0.003, 5.0961111111)
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

    if total_pipeline_latency < 60:
        return 1500
    elif total_pipeline_latency < 100:
        score = float(100 - total_pipeline_latency) / 40
        return 500 + (score * 1000)
    else:
        return 0

core_cost_tiers = [{"cores": 100, "cost": 5}, {"cores": None, "cost": 25}]

def core_tier(cores):

    for i, tier in enumerate(core_cost_tiers):
        if tier["cores"] is None:
            return i
        cores -= tier["cores"]
        if cores <= 0:
            return i

    raise Exception("Bad core_cost_tiers")

def concurrent_cores_hired_to_cost(cores):

    # Model a simple situation where we own a bunch of cores (which are thus nearly free)
    # but can also hire more from the cloud (more expensive)

    cost = 0.0

    for tier in core_cost_tiers:

        if cores == 0:
            break

        if tier["cores"] is None:
            cores_here = cores
        else:
            cores_here = min(cores, tier["cores"])

        cost += (cores_here * tier["cost"])
        cores -= cores_here

    return cost
    
dynamic_core_choices = [1, 2, 4]
dynamic_core_greed_factor = 1.5

vm_startup_delay = 0.5

def predicted_to_real_time(predicted_time):

    return predicted_time * random.normalvariate(1, 0.05)
