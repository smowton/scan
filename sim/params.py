
# The customisable, semi-fixed aspects of the simulation

import math
import random

# Processing times: how many time units will it take to process the given number of records?
# When threading or splitting are used, the record count still represents the entire job.
# There are basically two classes of task: the largely input-dependent, and the largely input-independent.

def processing_time(record_count, thread_count, split_count, phase_index, include_gather):
    
    params = stage_to_time_params[phase_index]
    ret = params[1] + (params[0] * (float(record_count) / split_count))
    if ret < 0:
        ret = 0.0

    ideal_thread_factor = float(1) / thread_count
    thread_constant_part = thread_factor_params[phase_index]

    ret *= (thread_constant_part + ((1 - thread_constant_part) * ideal_thread_factor))

    if include_gather and split_count > 1:
        ret += gather_time(record_count, phase_index)

    return ret

thread_factor_params = [
    0.111111111111,
    0.981389578164,
    0.312390924956,
    0.213333333333,
    0.0904925544101,
    0.75,
    0.976744186047
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

    standard_reward = 1800
    job_size_factor = float(record_count) / 5
    standard_reward *= job_size_factor

    score = float(120 - total_pipeline_latency) / 120
    return score * standard_reward

core_cost_tiers = [{"cores": 100, "cost": 5}, {"cores": None, "cost": 50}]

def core_tier(cores):

    for i, tier in enumerate(core_cost_tiers):
        if tier["cores"] is None:
            return i
        cores -= tier["cores"]
        if cores <= 0:
            return i

    raise Exception("Bad core_cost_tiers")

def cores_hired_by_tier(cores):

    cores_by_tier = []

    for tier in core_cost_tiers:

        if cores == 0:
            break

        if tier["cores"] is None:
            cores_here = cores
        else:
            cores_here = min(cores, tier["cores"])

        cores_by_tier.append(cores_here)
        cores -= cores_here

    return cores_by_tier

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
    
dynamic_core_choices = [1, 2, 4, 8, 16]

vm_startup_delay = 0.0

runtime_prediction_error = 0.0

def predicted_to_real_time(predicted_time):

    pred = predicted_time * random.normalvariate(1, runtime_prediction_error)
    realtime = min(max(predicted_time * 0.1, pred), predicted_time * 1.9)
    return realtime
