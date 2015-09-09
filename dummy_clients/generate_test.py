#!/usr/bin/env python

import generate_gatk
import generate_helpers
import generate_gromacs
import sys
import random

runid = random.randint(0, 1000000)

if sys.argv[1] == "gatk":

    generate_gatk.upload_gatk_refdata(sys.argv[2], sys.argv[3])
    pid = generate_gatk.start_gatk(sys.argv[2], sys.argv[4], 0.1, runid)
    generate_helpers.wait_for_task(sys.argv[2], pid)
    generate_gatk.cleanup_gatk(sys.argv[2], runid)

elif sys.argv[1] == "gromacs":

    generate_gromacs.start_gromacs(sys.argv[2], sys.argv[3], runid, 0.9) 

elif sys.argv[1] == "cellprofiler":

    pass
