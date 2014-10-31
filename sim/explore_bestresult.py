#!/usr/bin/env python

# Given a root directory, find the best result and explore every possible variation of *one* parameter from that position.

import sys
import bestresult
import hillclimb
import copy
import itertools

if len(sys.argv) < 2:
    print >>sys.stderr, "Usage: explore_bestresult.py data_directory"

best = bestresult.get_best_result_in(sys.argv[1])[0]

print "Exploring from best result", best

best_spec = best["spec"]
to_try = []

if best_spec["nmachines"][0] is not None:

    if len(best_spec["nmachines"]) == 1:

        current_val = best_spec["nmachines"][0]

        for new_n in range(0, (current_val * 2) + 1, 10):
            new_spec = copy.deepcopy(best_spec)
            new_spec["nmachines"][0] = new_n
            to_try.append(new_spec)

    else:

        for i in range(-10, 11):
            new_spec = copy.deepcopy(best_spec)

            for idx in range(len(best_spec["nmachines"])):
                new_spec["nmachines"][idx] += i
            
            to_try.append(new_spec)

if best_spec["machine_specs"][0] is not None:

    new_cores_l = itertools.product([1,2,4], repeat=len(best_spec["machine_specs"]))
    for new_cores in new_cores_l:

        new_spec = copy.deepcopy(best_spec)
        new_spec["machine_specs"] = list(new_cores)
        to_try.append(new_spec)

if best_spec["phase_splits"][0] is not None:

    new_splits_l = itertools.product([1,2,4], repeat=len(best_spec["phase_splits"]))
    for new_splits in new_splits_l:

        new_spec = copy.deepcopy(best_spec)
        new_spec["phase_splits"] = list(new_splits)
        to_try.append(new_spec)

to_try = filter(hillclimb.valid_params, to_try)

print "Trying", len(to_try), "variants"

hillclimb.workdir = sys.argv[1]

for t in to_try:
    hillclimb.start_trial(t)

hillclimb.await_running_tries()
