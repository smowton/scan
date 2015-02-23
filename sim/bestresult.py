#!/usr/bin/env python

# Find the strongest ten results matching some constraints.

import sys
import os
import os.path
import json
import numpy

def arg_to_dir(arg):
    return "-".join([str(int(x)) for x in arg.split(",")])

def get_dirs(match, within):

    if match is None:
        return os.listdir(within)
    else:
        if os.path.exists(os.path.join(within, match)):
            return [match]
        else:
            return []

def toint(v):
    if v == "None":
        return None
    else:
        return int(v)

def dirname_to_list(d):
    return [toint(v) for v in d.split("-")]

def get_best_result_in(root_dir, match_nmachines = None, match_cores = None, match_splits = None):

    results = []

    for splitdir in get_dirs(match_splits, root_dir):

        abssplitdir = os.path.join(root_dir, splitdir)
        for coredir in get_dirs(match_cores, abssplitdir):

            abscoredir = os.path.join(abssplitdir, coredir)
            for machinedir in get_dirs(match_nmachines, abscoredir):

                absncoresdir = os.path.join(abscoredir, machinedir)
                for ncoresdir in get_dirs(None, absncoresdir):

                    try:

                        def load_try(i):
                            with open(os.path.join(absncoresdir, ncoresdir, str(i), "out.json"), "r") as f:
                                return json.load(f)["avgprofit"]

                        tries = [load_try(i) for i in range(10)]
                        mean = numpy.mean(tries)
                        std = numpy.std(tries)
                        spec = {"phase_splits": splitdir, "machine_specs": coredir, "nmachines": machinedir}
                        for (k, v) in spec.items():
                            spec[k] = dirname_to_list(v)

                        results.append({"spec": spec, "mean": mean, "std": std})

                    except Exception as e:

                        print >>sys.stderr, "Skipping malformed result", os.path.join(abscoredir, machinedir), "(%s)" % e

    return sorted(results, key = lambda x : x["mean"], reverse = True)

if __name__ == "__main__":

    match_nmachines = None
    match_cores = None
    match_splits = None
    root_dir = None

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

    results = get_best_result_in(root_dir, match_nmachines, match_cores, match_splits)

    for result in results:

        json.dump(result, sys.stdout)
        sys.stdout.write("\n")
                
    
