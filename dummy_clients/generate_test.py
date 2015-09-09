#!/usr/bin/env python

import generate_gatk
import generate_helpers
import generate_gromacs
import generate_cp
import sys
import random

runid = random.randint(0, 1000000)

if sys.argv[1] == "gatk":

    generate_gatk.upload_gatk_refdata(sys.argv[2], sys.argv[3])
    pid = generate_gatk.start_gatk(sys.argv[2], sys.argv[4], random.uniform(0.08, 0.12), runid)
    generate_helpers.wait_for_task(sys.argv[2], pid)
    generate_gatk.get_results(sys.argv[2], runid, "gatk.out")
    generate_gatk.cleanup_gatk(sys.argv[2], runid)

elif sys.argv[1] == "gromacs":

    pid = generate_gromacs.start_gromacs(sys.argv[2], sys.argv[3], runid, random.uniform(0.08, 0.12)) 
    generate_helpers.wait_for_task(sys.argv[2], pid)
    generate_gromacs.get_results(sys.argv[2], runid, "gromacs.out")
    generate_gromacs.cleanup_gromacs(sys.argv[2], runid)

elif sys.argv[1] == "cellprofiler":

    pid = generate_cp.start_cp(sys.argv[2], sys.argv[3], runid, random.choice([2000, 2048, 2096]))
    generate_helpers.wait_for_task(sys.argv[2], pid)
    generate_cp.get_results(sys.argv[2], runid, "cp.out")
    generate_cp.cleanup_cp(sys.argv[2], runid)

else:
    
    raise Exception("Bad test type %s" % sys.argv[1])
