#!/usr/bin/python

import cqlscan
import sys

if len(sys.argv) < 2:
	print >>sys.stderr, "Usage: addvcf.py in.vcf [tag1=val1 tag2=val2 ...]"
	sys.exit(1)

with open(sys.argv[1], "r") as vcf:

	attribdict = dict()

	for vcfl in vcf:
		if vcfl.startswith("#"):
			continue
		bits = [x.strip() for x in vcfl.split("\t")]
		# Key: chrom-position-oldseq-newseq
		key = "\t".join([bits[0], bits[1], bits[3], bits[4]])
		# Val: the rest of the fields
		val = "\t".join(bits[5:])
		attribdict[key] = val

tok = cqlscan.cql_connect()
sampleid = cqlscan.cql_add_sample_tags(tok, attribdict, sys.argv[2:])
cqlscan.cql_close(tok)

print "Added sample", sampleid
