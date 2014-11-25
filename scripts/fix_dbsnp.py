#!/usr/bin/python

import sys

with open(sys.argv[1], "r") as f:

    for line in f:

        if line.startswith("#"):
            continue

        bits = line.split("\t")
        chrom = bits[0].strip()

        if chrom == "MT":

            print >>sys.stderr, "DBSNP is broken; fixing..."
            break

        elif chrom == "1":

            print >>sys.stderr, "DBSNP is OK"
            sys.exit(0)

        else:

            raise Exception("DBSNP is strange; bailing out")

subprocess.check_call(["./reorder_dbsnp.sh", sys.argv[1]])

