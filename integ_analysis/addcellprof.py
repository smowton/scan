#!/usr/bin/python

import cqlscan
import sys

if len(sys.argv) < 2:
	print >>sys.stderr, "Usage: addcellprof.py celldata.csv [tag1=val1 tag2=val2 ...]"
	sys.exit(1)

with open(sys.argv[1], "r") as cpf:

	attribdict = dict()
	cpdata = cpf.read()
	cpdata = cpdata.replace("\r\n", "\\n")
	cpdata = cpdata.replace("\n", "\\n")
	attribdict["imagedata"] = cpdata

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample_tags(tok, attribdict, sys.argv[2:])
cqlscan.cql_close(tok)

print "Added sample", sampleid
