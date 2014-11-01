#!/usr/bin/env python

# Given a root directory, find the best result and explore every possible variation of *one* parameter from that position.

import sys
import bestresult
import hillclimb
import copy
import itertools

def collect_machine_vars(best_spec):

    result = []

    if len(best_spec["nmachines"]) == 1:

        current_val = best_spec["nmachines"][0]

        for new_n in range(0, (current_val * 2) + 1, 10):
            new_spec = copy.deepcopy(best_spec)
            new_spec["nmachines"][0] = new_n
            result.append(new_spec)

    else:

        for i in range(-10, 11):
            new_spec = copy.deepcopy(best_spec)

            for idx in range(len(best_spec["nmachines"])):
                new_spec["nmachines"][idx] += i
            
            result.append(new_spec)

    return result

def collect_product_vars(best_spec, keyname, vals):

    result = []

    new_vals_l = itertools.product(vals, repeat=len(best_spec[keyname]))
    for new_vals in new_vals_l:

        new_spec = copy.deepcopy(best_spec)
        new_spec[keyname] = list(new_vals)
        result.append(new_spec)

    return result

def collect_spec_vars(best_spec):
    return collect_product_vars(best_spec, "machine_specs", [1,2,4])

def collect_split_vars(best_spec):
    vars = collect_product_vars(best_spec, "phase_splits", [1,2,4])
    if len(best_spec["nmachines"]) == 8:
        for v in vars:
            if all([x == 1 for x in v["phase_splits"]]):
                v["nmachines"].pop()
                v["machine_specs"].pop()
    elif len(best_spec["nmachines"]) == 7:
        for v in vars:
            if not all([x == 1 for x in v["phase_splits"]]):
                v["nmachines"].append(None if v["nmachines"][0] is None else 1)
                v["machine_specs"].append(None if v["machine_specs"][0] is None else 1)
    return vars

def collect_vars(best_spec):

    to_try = []

    if best_spec["nmachines"][0] is not None:
        to_try.extend(collect_machine_vars(best_spec))

    if best_spec["machine_specs"][0] is not None:
        to_try.extend(collect_spec_vars(best_spec))

    if best_spec["phase_splits"][0] is not None:
        to_try.extend(collect_split_vars(best_spec))

    to_try = filter(hillclimb.valid_params, to_try)

    return to_try

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print >>sys.stderr, "Usage: explore_bestresult.py data_directory"

    best = bestresult.get_best_result_in(sys.argv[1])[0]

    print "Exploring from best result", best

    best_spec = best["spec"]
    to_try = collect_vars(best_spec)

    print "Trying", len(to_try), "variants"

    hillclimb.workdir = sys.argv[1]

    for t in to_try:
        hillclimb.start_trial(t)

    hillclimb.await_running_tries()
