#!/usr/bin/python

import cqlscan
import sys
import json
import httplib

if len(sys.argv) < 3:
	print >>sys.stderr, "Usage: addvcf.py in.vcf sampleid [cassandra_host]"
	sys.exit(1)

with open(sys.argv[1], "r") as vcf:

	attribdict = dict()
        conn = httplib.HTTPConnection("rest.ensembl.org", 80, strict=False, timeout=5)

	for vcfl in vcf:
		if vcfl.startswith("#"):
			continue
		if vcfl.find("PASS") == -1:
			continue
		bits = [x.strip() for x in vcfl.split("\t")]
		# Key: chrom-position-oldseq-newseq
		key = "\t".join([bits[0], bits[1], bits[3], bits[4]])
		# Val: the rest of the fields
		val = "\t".join(bits[5:])
		attribdict[key] = val

tok = cqlscan.cql_connect(host = sys.argv[3])
sampleid = cqlscan.cql_add_sample(tok, attribdict, sys.argv[2])
cqlscan.cql_close(tok)

print "Added sample", sampleid
