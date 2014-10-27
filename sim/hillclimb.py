#!/usr/bin/python

import sim
import sys
import json
import os
import os.path
import tempfile
import time
import subprocess
import copy

import PBSPy.capi as pbs

torque = pbs.Server()
torque.connect()

workdir = sys.argv[1]
profile = sys.argv[2]
running_tries = set()

try_results = dict()

best_params = []
finished_params = []

n_tries = 10

queue_limit = 1000
queued_jobs = 0

class NoSuchTryException(Exception):
    pass

def job_getattr(j, name):

    for a in j.attribs:
        if a.name == name:
            return a.value
    return None
    
def count_queue():
    
    count = 0
    
    jobs = torque.statjob()
    for j in jobs:
        if job_getattr(j, "Job_Owner").startswith("csmowton") and job_getattr(j, "job_state") == "Q":
            count += 1

    global queued_jobs
    queued_jobs = count
    print "***", count, "jobs pending execution"
    return count

def wait_for_job_queue_space():

    count_queue()

    while queued_jobs > (queue_limit / 2):
        print "Too many (%d) jobs queued; taking a break..." % queued_jobs
        time.sleep(5)
        count_queue()
        
def params_tuple(p):
    return (tuple(p["nmachines"]), tuple(p["machine_specs"]), tuple(p["phase_splits"]))

def params_tuple_dict(p):
    
    return {"nmachines": list(p[0]),
            "machine_specs": list(p[1]),
            "phase_splits": list(p[2])}

def try_tuple(t):

    return (t["try"], params_tuple(t["params"]))

def try_tuple_dict(t):

    return {"try": t[0],
            "params": params_tuple_dict(t[1])}

def params(nmachines, cores, splits):
    return {"nmachines": nmachines,
            "machine_specs": cores,
            "phase_splits": splits}

def tries(nmachines, cores, splits):
    return [{"try": i, "params": params(nmachines, cores, splits)} for i in range(n_tries)]

def trydir(t):
    params = t["params"]
    return os.path.join(workdir, "%s/%s/%s/%d" % ("-".join([str(x) for x in params["phase_splits"]]),
                                                  "-".join([str(x) for x in params["machine_specs"]]),
                                                  "-".join([str(x) for x in params["nmachines"]]),
                                                  t["try"]))

def tryfile(t):
    return os.path.join(trydir(t), "out.json")

def starttry(t):

    global queued_jobs

    # Exaggerate the limit a little: some jobs have likely passed into the running state
    # in the meantime and we'll just thrash the check if we keep checking for exactly the limit value
    # when we've submitted that many jobs.
    if queued_jobs > (queue_limit * 1.1):
        wait_for_job_queue_space()

    try:
        os.makedirs(trydir(t))
    except OSError as e:
        if e.errno != 17:
            raise e

    shellfile = os.path.join(trydir(t), "run.sh")

    params = t["params"]

    if params["nmachines"][0] is None:
        nmachines_param = ""
    else:
        nmachines_param = "nmachines=" + ",".join([str(x) for x in params["nmachines"]])

    if params["machine_specs"][0] is None:
        specs_param = ""
    else:
        specs_param = "machine_specs=" + ",".join([str(x) for x in params["machine_specs"]])

    splits_param = "phase_splits=" + ",".join([str(x) for x in params["phase_splits"]])

    command = ["python", "/home/csmowton/tmp/scan/sim/driver.py", nmachines_param, specs_param, splits_param, "noplot", "json"]
    command = filter(lambda x: len(x) > 0, command)

    with open(shellfile, "w") as f:
        f.write("%s > %s" % (" ".join(command), tryfile(t)))
        
    print "Start try", t

    attribs = [
        pbs.Attr("Output_Path", tryfile(t) + ".err"),
        pbs.Attr("Join_Path", "true"),
        pbs.Attr("PBS_O_WORKDIR", trydir(t))
    ]

    while True:
        try:
            torque.submit(attribs, shellfile)
            break
        except Exception as e:
            print "Submission failed", e
            print "Taking a minute's break..."
            time.sleep(60)
    #start_proc(["/lustre/gridware/pkg/clusterware/torque/4.2.6.1/gcc/4.4.7/bin/qsub", "-e", tryfile(t) + ".err", "-d", trydir(t), "-j", "eo", shellfile])
    
    queued_jobs += 1

    running_tries.add(try_tuple(t))

def read_try_result(t):

    try:

        with open(tryfile(t), "r") as f:

            result = json.load(f)
            if "reward" not in result or "ratio" not in result or "cost" not in result:
                raise NoSuchTryException("Not finished, or bad JSON?")
            return result

    except IOError as e:
        raise NoSuchTryException(e)
    except ValueError as e:
        raise NoSuchTryException(e)

def check_running_tries():

    done = []

    check_tries = copy.deepcopy(running_tries)
    for ttup in check_tries:

        t = try_tuple_dict(ttup)

        try:

            result = read_try_result(t)
            try_results[ttup] = result
            done.append(ttup)
            check_finished_param(t["params"])

        except NoSuchTryException as e:
            continue

    for d in done:

        running_tries.remove(d)

    return len(done) > 0

def up_nmachines(p, idx):

    newp = copy.deepcopy(p)
    oldval = newp["nmachines"][idx]
    if oldval == None:
        return None
    if oldval < 20:
        interval = 2
    elif oldval < 60:
        interval = 5
    else:
        interval = 10
    newp["nmachines"][idx] += interval
    return newp

def up_cores(p, idx):

    if p["machine_specs"][idx] == None:
        return None
    newp = copy.deepcopy(p)
    newp["machine_specs"][idx] *= 2
    if newp["nmachines"][idx] is not None:
        newp["nmachines"][idx] /= 2
    return newp

def up_split(p, idx):

    newp = copy.deepcopy(p)
    newp["phase_splits"][idx] *= 2
    if len(newp["machine_specs"]) == 7:
        # Must start specifying the gather phase
        if newp["machine_specs"][0] is not None:
            newp["machine_specs"].append(1)
        else:
            newp["machine_specs"].append(None)
        if newp["nmachines"][0] is not None:
            newp["nmachines"].append(2)
        else:
            newp["nmachines"].append(None)
    return newp

def valid_params(p):

    if p == None:
        return False

    for i in p["machine_specs"]:
        if i == 0 or i > 4:
            return False
    for i in p["phase_splits"]:
        if i == 0 or i > 4:
            return False
    for i in p["nmachines"]:
        if i == 0:
            return False

    # Phase 7 never splits
    if p["phase_splits"][6] != 1:
        return False

    # Stages 2 and 6, and gather do not support multiple cores
    if len(p["machine_specs"]) > 1:
        if p["machine_specs"][1] > 1 or p["machine_specs"][5] > 1:
            return False
        if len(p["machine_specs"]) >= 8 and p["machine_specs"][7] > 1:
            return False

    if disable_splits:
        for i in p["phase_splits"]:
            if i != 1:
                return False

    if disable_multicore:
        for i in p["machine_specs"]:
            if i != 1:
                return False

    return True

def up_from(p):

    # All the ways of climbing up:
    neighbours = [up_nmachines(p, idx) for idx in range(len(p["nmachines"]))] + \
        [up_cores(p, idx) for idx in range(len(p["machine_specs"]))] + \
        [up_split(p, idx) for idx in range(len(p["phase_splits"]))]    

    return filter(valid_params, neighbours)

def param_result(p):
    try:
        return float(sum([try_results[try_tuple({"try": i, "params": p})]["ratio"] for i in range(n_tries)])) / n_tries
    except KeyError:
        return None

def check_finished_param(p):

    for i in range(n_tries):

        if try_tuple({"try": i, "params": p}) not in try_results:
            return

    print "All tries for", p, "completed"
    finished_params.append(p)

def start_trial(p, parent_params):

    found_old_results = False
    already_ran_this_session = False

    for i in range(n_tries):

        t = {"try": i, "params": p}
        ttup = try_tuple(t)

        #print "Consider try", t, ttup

        if ttup in running_tries:
            #print "Try already in running_tries"
            already_ran_this_session = True
            continue
        if ttup in try_results:
            #print "Try already in try_results"
            already_ran_this_session = True
            continue

        try:
            result = read_try_result(t)
            # Will find the results on the next pass.
            # Note the try "in progress," but don't actually start it.
            running_tries.add(try_tuple(t))            
            found_old_results = True
            #print "Try results read from disk", result
            continue
        except Exception as e:
            #print "Failed to read try results", e
            pass

        #print "Start try"
        starttry(t)

    if found_old_results:
        print "Found old results for", p, ": some tries skipped"
        
def hillclimb_from(p):

    neighbours = up_from(p)

    for n in neighbours:
        start_trial(n, p)

# Main:

disable_splits = False
disable_multicore = False

max_search_splay = 10

if profile == "noflex-multiqueue":
    init_params = {"nmachines": [12,12,12,12,12,4,12], "machine_specs": [1] * 7, "phase_splits": [1] * 7}
elif profile == "noflex":
    init_params = {"nmachines": [80], "machine_specs": [1], "phase_splits": [1] * 7}
elif profile == "horiz":
    init_params = {"nmachines": [None], "machine_specs": [1], "phase_splits": [1] * 7}
elif profile == "horiz-multiqueue":
    init_params = {"nmachines": [None] * 7, "machine_specs": [1] * 7, "phase_splits": [1] * 7}
elif profile == "vert":
    init_params = {"nmachines": [None], "machine_specs": [None], "phase_splits": [1] * 7}
else:
    raise Exception("Bad profile %s" % profile)


start_trial(init_params, None)

while True:

    while len(running_tries) > 0:

        print "***", len(running_tries), "tries alive ***"
        progress = check_running_tries()

        if not progress:
            time.sleep(5)

    finished_param_results = [(p, param_result(p)) for p in finished_params + best_params]
    finished_params = []
    new_best_params = sorted(finished_param_results, key = lambda t: t[1], reverse = True)[:10]
    
    print "Wave complete. Best candidates:"

    for (param, result) in new_best_params:
        print result, param

    if len(new_best_params) == len(best_params) and all([new_param == old_param for ((new_param, new_result), old_param) in zip(new_best_params, best_params)]):
        print "No progress. Stop."
        break

    best_params = [x[0] for x in new_best_params]

    for b in best_params:
        hillclimb_from(b)
