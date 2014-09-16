#!/usr/bin/python

import cqlscan
import sys
import csv
import json
import re

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: addpeptides.py evidence.txt sampleid"
	sys.exit(1)

with open(sys.argv[1], "r") as evf:

	attribdict = dict()
	evr = csv.reader(evf, delimiter='\t')

	headers = evr.next()
	
	for evl in evr:
		key = evl[0]
		valdict = { headers[i]: evl[i] for i in range(1, len(headers)) }
                # Add raw data:
		val = json.dumps(valdict)
		attribdict[key] = val
                # Add derived data: leading-protein -> intensity:
                prots = re.split(",|;", valdict["Leading Proteins"])
                for prot in prots:
                        attribdict["%s-intensity" % prot] = attribdict["Intensity"]

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample(tok, attribdict, sys.argv[2])
cqlscan.cql_close(tok)

print "Added sample", sampleid
