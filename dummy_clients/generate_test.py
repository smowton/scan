#!/usr/bin/env python

import generate_gatk
import generate_helpers
import sys
import random

runid = random.randint(0, 1000000)

generate_gatk.upload_gatk_refdata(sys.argv[1], sys.argv[2])
pid = generate_gatk.start_gatk(sys.argv[1], sys.argv[3], 0.1, runid)
generate_helpers.wait_for_task(sys.argv[1], pid)
generate_gatk.cleanup_gatk(sys.argv[1], runid)

