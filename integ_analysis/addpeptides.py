#!/usr/bin/python

import cqlscan
import sys
import csv
import json

if len(sys.argv) < 2:
	print >>sys.stderr, "Usage: addpeptides.py evidence.txt [tag1=val1 tag2=val2 ...]"
	sys.exit(1)

with open(sys.argv[1], "r") as evf:

	attribdict = dict()
	evr = csv.reader(evf, delimiter='\t')

	headers = evr.next()
	
	for evl in evr:
		key = evl[0]
		valdict = { headers[i]: evl[i] for i in range(1, len(headers)) }
		val = json.dumps(valdict)
		attribdict[key] = val

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample_tags(tok, attribdict, sys.argv[2:])
cqlscan.cql_close(tok)

print "Added sample", sampleid
