#!/usr/bin/env python

# Find the strongest ten results matching some constraints.

import sys
import os
import os.path
import json

match_nmachines = None
match_cores = None
match_splits = None
root_dir = None

def arg_to_dir(arg):
    return "-".join([str(int(x)) for x in arg.split(",")])

for arg in sys.argv[1:]:

    arg = arg.split("=")
    if len(arg) != 2:
        raise Exception("Bad argument %s" % arg)

    if arg[0] == "root":
        root_dir = arg[1]
        continue

    argval = arg_to_dir(arg[1])

    if arg[0] == "nmachines":
        match_nmachines = argval
    elif arg[0] == "machine_specs":
        match_cores = argval
    elif arg[0] == "phase_splits":
        match_splits = argval
    else:
        raise Exception("Bad arg", arg)
    

results = []

def get_dirs(match, within):

    if match is None:
        return os.listdir(within)
    else:
        if os.path.exists(os.path.join(within, match)):
            return [match]
        else:
            return []

for splitdir in get_dirs(match_splits, root_dir):

    abssplitdir = os.path.join(root_dir, splitdir)
    for coredir in get_dirs(match_cores, abssplitdir):

        abscoredir = os.path.join(abssplitdir, coredir)
        for machinedir in get_dirs(match_nmachines, abscoredir):

            def load_try(i):
                with open(os.path.join(abscoredir, machinedir, str(i), "out.json"), "r") as f:
                    return json.load(f)["ratio"]
            
            result = sum([load_try(i) for i in range(10)]) / 10

            results.append({"spec": {"splits": splitdir, "cores": coredir, "machines": machinedir}, "result": result})

results = sorted(results, key = lambda x : x["result"], reverse = True)

for result in results:

    print result["spec"], result["result"]
                
    
